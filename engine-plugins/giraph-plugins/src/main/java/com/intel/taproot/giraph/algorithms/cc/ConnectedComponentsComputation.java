/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.giraph.algorithms.cc;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.counters.GiraphStats;
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

    @Override
    public void preSuperstep() {
        convergenceProgressOutputInterval = getConf().getInt(CONVERGENCE_CURVE_OUTPUT_INTERVAL, 1);
        if (convergenceProgressOutputInterval < 1) {
            throw new IllegalArgumentException("Convergence curve output interval should be >= 1.");
        }
    }

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
    public static class ConnectedComponentsAggregatorWriter implements AggregatorWriter {
        /**
         * Name of the file we wrote to
         */
        private static String FILENAME;
        /**
         * Saved output stream to write to
         */
        private FSDataOutputStream output;
        /**
         * Last superstep number
         */
        private long lastStep = 0L;

        private ImmutableClassesGiraphConfiguration conf;

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
            long realStep = lastStep;

            if (superstep == 0) {
                output.writeBytes("======Graph Statistics======\n");
                output.writeBytes(String.format("Number of vertices: %d%n",
                    GiraphStats.getInstance().getVertices().getValue()));
                output.writeBytes(String.format("Number of edges: %d%n",
                    GiraphStats.getInstance().getEdges().getValue()));
                output.writeBytes("\n");
                output.writeBytes("======Connected Components Configuration======\n");
                output.writeBytes(String.format("convergenceProgressOutputInterval: %d%n",
                    convergenceProgressOutputInterval));
                output.writeBytes("\n");
                output.writeBytes("======Convergence Progress======\n");
            } else if (realStep >= 0 && realStep % convergenceProgressOutputInterval == 0) {
                // output learning progress
                double sumDelta = Double.parseDouble(map.get(SUM_DELTA));
                double numUpdates = Double.parseDouble(map.get(SUM_UPDATES));
                if (numUpdates > 0) {
                    double avgUpdates = sumDelta / numUpdates;
                    output.writeBytes(String.format("superstep = %d%c", realStep, '\t'));
                    output.writeBytes(String.format("avgDelta = %f%n", avgUpdates));
                }
            }
            output.flush();
            lastStep = superstep;
        }

        @Override
        public void close() throws IOException {
            output.close();
        }

        /**
         * Set the configuration to be used by this object.
         *
         * @param configuration Set configuration
         */
        @Override
        public void setConf(ImmutableClassesGiraphConfiguration configuration) {
            this.conf = configuration;
        }

        /**
         * Return the configuration used by this object.
         *
         * @return Set configuration
         */
        @Override
        public ImmutableClassesGiraphConfiguration getConf() {
            return conf;
        }

    }
}