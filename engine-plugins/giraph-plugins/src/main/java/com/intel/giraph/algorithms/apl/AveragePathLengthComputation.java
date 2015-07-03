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

package com.intel.taproot.giraph.algorithms.apl;

import com.intel.taproot.giraph.io.DistanceMapWritable;
import com.intel.taproot.giraph.io.HopCountWritable;
import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Average path length calculation.
 * http://en.wikipedia.org/wiki/Average_path_length
 */
@Algorithm(
        name = "Average path length",
        description = "Finds average of shortest path lengths between all pairs of nodes."
)

public class AveragePathLengthComputation extends BasicComputation
        <LongWritable, DistanceMapWritable, NullWritable, HopCountWritable> {

    /**
     * Custom argument for convergence progress output interval (default: every superstep)
     */
    public static final String CONVERGENCE_CURVE_OUTPUT_INTERVAL = "apl.convergenceProgressOutputInterval";
    /**
     * Aggregator name on sum of delta values
     */
    private static String SUM_DELTA = "sumDelta";
    /**
     * Aggregator name on sum of delta values
     */
    private static String SUM_UPDATES = "sumUpdates";
    /**
     * Iteration interval to output learning curve
     */
    private int convergenceProgressOutputInterval = 1;


    /**
     * Flood message to all its direct neighbors with a new distance value.
     *
     * @param vertex      Vertex
     * @param source      Source vertex ID.
     * @param newDistance Distance from source to the next destination.
     */
    private void floodMessage(Vertex<LongWritable, DistanceMapWritable, NullWritable> vertex,
                              long source, int newDistance) {
        for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
            sendMessage(edge.getTargetVertexId(), new HopCountWritable(source, newDistance));
        }
    }

    @Override
    public void preSuperstep() {
        convergenceProgressOutputInterval = getConf().getInt(CONVERGENCE_CURVE_OUTPUT_INTERVAL, 1);
        if (convergenceProgressOutputInterval < 1) {
            throw new IllegalArgumentException("Convergence curve output interval should be >= 1.");
        }
    }

    /**
     * algorithm compute
     *
     * @param vertex   Giraph Vertex
     * @param messages Giraph messages
     */
    @Override
    public void compute(Vertex<LongWritable, DistanceMapWritable, NullWritable> vertex,
                        Iterable<HopCountWritable> messages) {

        // initial condition - start with sending message to all its neighbors
        if (getSuperstep() == 0) {
            floodMessage(vertex, vertex.getId().get(), 1);
            vertex.voteToHalt();
            return;
        }

        // Process every message received from its direct neighbors
        for (HopCountWritable message : messages) {
            // source vertex id
            long source = message.getSource();

            if (source == vertex.getId().get()) {
                // packet returned to the original sender
                continue;
            }

            // distance between source and current vertex
            int distance = message.getDistance();

            DistanceMapWritable vertexValue = vertex.getValue();
            if ((vertexValue.distanceMapContainsKey(source) &&
                 vertexValue.distanceMapGet(source) > distance) ||
                (!vertexValue.distanceMapContainsKey(source))) {
                double delta;
                if (vertexValue.distanceMapContainsKey(source)) {
                    delta = (double) (vertexValue.distanceMapGet(source) - distance);
                } else {
                    delta = (double) distance;
                }
                aggregate(SUM_DELTA, new DoubleWritable(delta));
                aggregate(SUM_UPDATES, new DoubleWritable(1d));
                vertex.getValue().distanceMapPut(source, distance);
                floodMessage(vertex, source, distance + 1);
            }
        }
        vertex.voteToHalt();
    }

    /**
     * Master compute associated with {@link AveragePathLengthComputation}.
     * It registers required aggregators.
     */
    public static class AveragePathLengthMasterCompute extends
        DefaultMasterCompute {
        /**
         * It registers aggregators for page rank
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
     * This is an aggregator writer for Page Rank.
     * It updates the convergence progress at the end of each super step
     */
    public static class AveragePathLengthAggregatorWriter extends DefaultImmutableClassesGiraphConfigurable
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
         * Last superstep number
         */
        private long lastStep = 0L;

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
            FILENAME = "apl-convergence-report_" + applicationAttempt;
        }

        @Override
        public void writeAggregator(Iterable<Map.Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {
            // collect aggregator data
            HashMap<String, String> map = new HashMap<String, String>();
            for (Map.Entry<String, Writable> entry : aggregatorMap) {
                map.put(entry.getKey(), entry.getValue().toString());
            }
            long realStep = lastStep;
            int convergenceProgressOutputInterval = getConf().getInt(CONVERGENCE_CURVE_OUTPUT_INTERVAL, 1);
            if (superstep == 0) {
                output.writeBytes("======Graph Statistics======\n");
                output.writeBytes(String.format("Number of vertices: %d%n",
                    GiraphStats.getInstance().getVertices().getValue()));
                output.writeBytes(String.format("Number of edges: %d%n",
                    GiraphStats.getInstance().getEdges().getValue()));
                output.writeBytes("\n");
                output.writeBytes("======Average Path Length Configuration======\n");
                output.writeBytes(String.format("convergenceProgressOutputInterval: %d%n",
                    convergenceProgressOutputInterval));
                output.writeBytes("\n");
                output.writeBytes("======Convergence Progress======\n");
            } else if (realStep > 0 && realStep % convergenceProgressOutputInterval == 0) {
                // output learning progress
                double sumDelta = Double.parseDouble(map.get(SUM_DELTA));
                double numUpdates = Double.parseDouble(map.get(SUM_UPDATES));
                if (numUpdates > 0) {
                    double avgUpdates = sumDelta / numUpdates;
                    output.writeBytes(String.format("superstep = %d%c", realStep, '\t'));
                    output.writeBytes(String.format("numUpdates = %f%c", numUpdates, '\t'));
                    output.writeBytes(String.format("sumDelta = %f%c", sumDelta, '\t'));
                    output.writeBytes(String.format("avgUpdates = %f%n", avgUpdates));
                }
            }
            output.flush();
            lastStep = superstep;
        }

        @Override
        public void close() throws IOException {
            output.close();
        }

    }
}
