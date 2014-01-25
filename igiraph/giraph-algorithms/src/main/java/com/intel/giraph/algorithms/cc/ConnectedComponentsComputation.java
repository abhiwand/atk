//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////
package com.intel.giraph.algorithms.cc;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The Connected Components algorithm which finds connected components
 * in large graphs.
 * <p/>
 * It is the HCC algorithm proposed by U Kang, Charalampos Tsourakakis
 * and Christos Faloutsos in "PEGASUS: Mining Peta-Scale Graphs", 2010
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 * <p/>
 * <p/>
 * This implementation is based on ConnectedComponentsComputation in giraph-examples
 * Updated data type
 * Added convergence progress curve
 * Added convergence threshold as one of stopping criteria
 * Added configurable parameters for use to control algorithm from
 * command line.
 * <p/>
 */
@Algorithm(
    name = "Connected components"
)
public class ConnectedComponentsComputation extends
    BasicComputation<LongWritable, LongWritable, NullWritable, LongWritable> {
    /**
     * Custom argument for convergence progress output interval (default: every superstep)
     */
    public static final String CONVERGENCE_CURVE_OUTPUT_INTERVAL = "cc.convergenceProgressOutputInterval";
    /**
     * Logger
     */
    private static final Logger LOG =
        Logger.getLogger(ConnectedComponentsComputation.class);
    /**
     * Aggregator name on sum of delta values
     */
    private static String SUM_DELTA = "sumDelta";
    /**
     * Aggregator name on num of vertex updates
     */
    private static String SUM_UPDATES = "sumUpdates";
    /**
     * Iteration interval to output learning curve
     */
    private int convergenceProgressOutputInterval = 1;

    /**
     * For each iteration, each node sends its current componentId
     * to its neighbors who are with higher componentId
     *
     * @param vertex   Vertex
     * @param messages Iterator of messages from the previous superstep.
     * @throws IOException
     */
    @Override
    public void compute(
        Vertex<LongWritable, LongWritable, NullWritable> vertex,
        Iterable<LongWritable> messages) throws IOException {
        convergenceProgressOutputInterval = getConf().getInt(CONVERGENCE_CURVE_OUTPUT_INTERVAL, 1);
        if (convergenceProgressOutputInterval < 1) {
            throw new IllegalArgumentException("Convergence curve output interval should be >= 1.");
        }

        long componentId;

        boolean changed = false;
        // for the first step, new value is from proactively inspecting neighbors
        if (getSuperstep() == 0) {
            //the initial componentId is the vertex id
            componentId = vertex.getId().get();
            vertex.setValue(new LongWritable(componentId));
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                long neighbor = edge.getTargetVertexId().get();
                if (neighbor < componentId) {
                    componentId = neighbor;
                    changed = true;
                }
            }
        } else {
        //for the reset steps, new value is from messages
            componentId = vertex.getValue().get();
            for (LongWritable message : messages) {
                long inputComponentId = message.get();
                if (inputComponentId < componentId) {
                    componentId = inputComponentId;
                    changed = true;
                }
            }
        }

        // propagate new value to the neighbors only when needed
        if (changed) {
            double delta = (double) Math.abs(vertex.getValue().get() - componentId);
            aggregate(SUM_DELTA, new DoubleWritable(delta));
            aggregate(SUM_UPDATES, new DoubleWritable(1d));
            vertex.setValue(new LongWritable(componentId));
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                LongWritable neighbor = edge.getTargetVertexId();
                if (neighbor.get() > componentId) {
                    sendMessage(neighbor, vertex.getValue());
                }
            }
        }
        vertex.voteToHalt();
    }

    /**
     * Master compute associated with {@link ConnectedComponentsComputation}.
     * It registers required aggregators.
     */
    public static class ConnectedComponentsMasterCompute extends
        DefaultMasterCompute {
        /**
         * It registers aggregators for Connected Components
         *
         * @throws InstantiationException
         * @throws IllegalAccessException
         */
        @Override
        public void initialize() throws InstantiationException,
            IllegalAccessException {
            registerAggregator(SUM_DELTA, DoubleSumAggregator.class);
            registerAggregator(SUM_UPDATES, DoubleSumAggregator.class);
        }
    }

    /**
     * This is an aggregator writer for Connected Components.
     * It updates the convergence progress at the end of each super step
     */
    public static class ConnectedComponentsAggregatorWriter extends DefaultImmutableClassesGiraphConfigurable
        implements AggregatorWriter {
        /**
         * Name of the file we wrote to
         */
        private static String FILENAME;
        /**
         * Saved output stream to write to
         */
        private FSDataOutputStream output;
        /**
         * super step number
         */
        private int lastStep = 0;

        public static String getFilename() {
            return FILENAME;
        }

        @SuppressWarnings("rawtypes")
        /**
         * create output file for convergence report
         */
        @Override
        public void initialize(Context context, long applicationAttempt) throws IOException {
            setFilename(applicationAttempt);
            String outputDir = context.getConfiguration().get("mapred.output.dir");
            Path path = new Path(outputDir + "/" + FILENAME);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
            output = fs.create(path, true);
        }

        /**
         * Set filename written to
         *
         * @param applicationAttempt app attempt
         */
        private static void setFilename(long applicationAttempt) {
            FILENAME = "cc-convergence-report_" + applicationAttempt;
        }

        @Override
        public void writeAggregator(Iterable<Map.Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {
            // collect aggregator data
            HashMap<String, String> map = new HashMap<String, String>();
            for (Map.Entry<String, Writable> entry : aggregatorMap) {
                map.put(entry.getKey(), entry.getValue().toString());
            }

            int convergenceProgressOutputInterval = getConf().getInt(CONVERGENCE_CURVE_OUTPUT_INTERVAL, 1);
            int realStep = lastStep;

            if (superstep == 0) {
                output.writeBytes("==================Connected Components Configuration====================\n");
                output.writeBytes("convergenceProgressOutputInterval: " + convergenceProgressOutputInterval + "\n");
                output.writeBytes("-------------------------------------------------------------\n");
                output.writeBytes("\n");
                output.writeBytes("===================Convergence Progress======================\n");
            } else if (realStep >= 0 && realStep % convergenceProgressOutputInterval == 0) {
                // output learning progress
                double sumDelta = Double.parseDouble(map.get(SUM_DELTA));
                double numUpdates = Double.parseDouble(map.get(SUM_UPDATES));
                if (numUpdates > 0) {
                    double avgUpdates = sumDelta / numUpdates;
                    output.writeBytes("superstep = " + realStep + "\tavgDelta = " + avgUpdates + "\n");
                }
            }
            output.flush();
            lastStep = (int) superstep;
        }

        @Override
        public void close() throws IOException {
            output.close();
        }

    }
}
