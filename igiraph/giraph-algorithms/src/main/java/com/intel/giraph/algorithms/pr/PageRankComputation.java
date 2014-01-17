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
package com.intel.giraph.algorithms.pr;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * The PageRank algorithm, http://en.wikipedia.org/wiki/PageRank
 * <p/>
 * It is based on SimplePageRankComputation in giraph-examples
 * Added convergence progress curve
 * Added convergence threshold as one of stopping criteria
 * Added configurable parameters for use to control algorithm from
 * command line.
 * <p/>
 * Remove the vertex input/output format from computation
 */
@Algorithm(
    name = "Page rank"
)
public class PageRankComputation extends BasicComputation<LongWritable,
    DoubleWritable, FloatWritable, DoubleWritable> {
    /**
     * Custom argument for number of super steps
     */
    public static final String MAX_SUPERSTEPS = "pr.maxSupersteps";
    /**
     * Custom argument for the convergence threshold
     */
    public static final String CONVERGENCE_THRESHOLD = "pr.convergenceThreshold";
    /**
     * Custom argument for the reset probability
     */
    public static final String RESET_PROBABILITY = "pr.resetProbability";
    /**
     * Custom argument for convergence progress output interval (default: every superstep)
     */
    public static final String CONVERGENCE_CURVE_OUTPUT_INTERVAL = "pr.convergenceProgressOutputInterval";
    /**
     * Custom argument for enable detailed progress report (default: false)
     */
    public static final String ENABLE_DETAILED_REPORT = "pr.enableDetailedReport";
    /**
     * Logger
     */
    private static final Logger LOG =
        Logger.getLogger(PageRankComputation.class);
    /**
     * Sum aggregator name
     */
    private static String SUM_AGG = "sum";
    /**
     * Aggregator name on sum of delta values
     */
    private static String SUM_DELTA_AGG = "sumDelta";
    /**
     * Min aggregator name
     */
    private static String MIN_AGG = "min";
    /**
     * Max aggregator name
     */
    private static String MAX_AGG = "max";
    /**
     * Number of super steps
     */
    private int maxSupersteps = 30;
    /**
     * The convergence threshold parameter
     */
    private float convergenceThreshold = 0.0001f;
    /**
     * The reset probability
     */
    private float resetProbability = 0.15f;
    /**
     * Iteration interval to output learning curve
     */
    private int convergenceProgressOutputInterval = 1;
    /**
     * Iteration interval to output learning curve
     */
    private boolean enableDetailedReport = false;

    @Override
    public void compute(
        Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {

        maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 30);
        if (maxSupersteps < 0) {
            throw new IllegalArgumentException("Number of super steps shoudl be > 0!");
        }

        convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.0001f);
        if (convergenceThreshold > 1) {
            throw new IllegalArgumentException("Convergence threshold should be at least <= 1");
        }

        resetProbability = getConf().getFloat(RESET_PROBABILITY, 0.15f);
        if (resetProbability < 0 || resetProbability > 1) {
            throw new IllegalArgumentException("Reset probability should be in [0,1] range");
        }

        convergenceProgressOutputInterval = getConf().getInt(CONVERGENCE_CURVE_OUTPUT_INTERVAL, 1);
        if (convergenceProgressOutputInterval < 1) {
            throw new IllegalArgumentException("Learning curve output interval should be >= 1.");
        }

        double delta = 0;

        if (getSuperstep() >= 1) {
            double sum = 0;
            for (DoubleWritable message : messages) {
                sum += message.get();
            }
            double newValue = (resetProbability / getTotalNumVertices()) + (1 - resetProbability) * sum;
            delta = Math.abs(vertex.getValue().get() - newValue);
            DoubleWritable vertexValue = new DoubleWritable(newValue);
            vertex.setValue(vertexValue);
            aggregate(SUM_DELTA_AGG, new DoubleWritable(delta));
            if (enableDetailedReport) {
                aggregate(MAX_AGG, vertexValue);
                aggregate(MIN_AGG, vertexValue);
                aggregate(SUM_AGG, new LongWritable(1));
            }
        }

        if (getSuperstep() == 0 ||
            (getSuperstep() < maxSupersteps &&
                delta > convergenceThreshold)) {
            long numEdges = vertex.getNumEdges();
            sendMessageToAllEdges(vertex,
                new DoubleWritable(vertex.getValue().get() / numEdges));
        } else {
            vertex.voteToHalt();
        }
    }

    /**
     * Worker context used with {@link PageRankComputation}.
     */
    public static class PageRankWorkerContext extends
        WorkerContext {
        /**
         * Final max value for verification for local jobs
         */
        private static double FINAL_MAX;
        /**
         * Final min value for verification for local jobs
         */
        private static double FINAL_MIN;
        /**
         * Final sum value for verification for local jobs
         */
        private static long FINAL_SUM;
        /**
         * Final sum delta value for learning curve output
         */
        private static double FINAL_SUM_DELATA;

        public static double getFinalMax() {
            return FINAL_MAX;
        }

        public static double getFinalMin() {
            return FINAL_MIN;
        }

        public static long getFinalSum() {
            return FINAL_SUM;
        }

        @Override
        public void preApplication()
            throws InstantiationException, IllegalAccessException {
        }

        @Override
        public void postApplication() {
            boolean enableDetailedReport = super.getContext().getConfiguration()
                .getBoolean(ENABLE_DETAILED_REPORT, false);
            FINAL_SUM_DELATA = this.<DoubleWritable>getAggregatedValue(SUM_DELTA_AGG).get();
            FINAL_SUM = this.<LongWritable>getAggregatedValue(SUM_AGG).get();
            FINAL_MAX = this.<DoubleWritable>getAggregatedValue(MAX_AGG).get();
            FINAL_MIN = this.<DoubleWritable>getAggregatedValue(MIN_AGG).get();
            if (enableDetailedReport) {
                LOG.info("aggregatedNVerticesValueChange=" + FINAL_SUM_DELATA);
                LOG.info("aggregatedNumVertices=" + FINAL_SUM);
                LOG.info("aggregatedMaxPageRank=" + FINAL_MAX);
                LOG.info("aggregatedMinPageRank=" + FINAL_MIN);
            }
        }

        @Override
        public void preSuperstep() {
            if (getSuperstep() >= 1) {
                DoubleWritable sumDelta = getAggregatedValue(SUM_DELTA_AGG);

                boolean detailedReport = super.getContext().getConfiguration()
                    .getBoolean(ENABLE_DETAILED_REPORT, false);
                if (detailedReport) {
                    LOG.info("aggregatedNumVertices=" +
                        getAggregatedValue(SUM_AGG) +
                        " NumVertices=" + getTotalNumVertices());
                    if (this.<LongWritable>getAggregatedValue(SUM_AGG).get() !=
                        getTotalNumVertices()) {
                        throw new RuntimeException("wrong value of SumAggreg: " +
                            getAggregatedValue(SUM_AGG) + ", should be: " +
                            getTotalNumVertices());
                    }
                    LOG.info("aggregatedVertexValueDelta=" + sumDelta.get());
                    DoubleWritable maxPagerank = getAggregatedValue(MAX_AGG);
                    LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
                    DoubleWritable minPagerank = getAggregatedValue(MIN_AGG);
                    LOG.info("aggregatedMinPageRank=" + minPagerank.get());
                }
            }
        }

        @Override
        public void postSuperstep() {
        }
    }

    /**
     * Master compute associated with {@link PageRankComputation}.
     * It registers required aggregators.
     */
    public static class PageRankMasterCompute extends
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
            registerAggregator(SUM_DELTA_AGG, DoubleSumAggregator.class);
            boolean enableDetailedReport = getConf().getBoolean(ENABLE_DETAILED_REPORT, false);
            if (enableDetailedReport) {
                registerAggregator(SUM_AGG, LongSumAggregator.class);
                registerPersistentAggregator(MIN_AGG, DoubleMinAggregator.class);
                registerPersistentAggregator(MAX_AGG, DoubleMaxAggregator.class);
            }
        }
    }

    /**
     * This is an aggregator writer for Page Rank.
     * It updates the convergence progress at the end of each super step
     */
    public static class PageRankAggregatorWriter extends DefaultImmutableClassesGiraphConfigurable
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
            FILENAME = "pr-convergence-report_" + applicationAttempt;
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
            int maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 20);
            int realStep = lastStep;

            if (superstep == 0) {
                float convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.0001f);
                float resetProbability = getConf().getFloat(RESET_PROBABILITY, 0.15f);
                output.writeBytes("==================Page Rank Configuration====================\n");
                output.writeBytes("maxSupersteps: " + maxSupersteps + "\n");
                output.writeBytes("convergenceThreshold: " + convergenceThreshold + "\n");
                output.writeBytes("resetProbability: " + resetProbability + "\n");
                output.writeBytes("convergenceProgressOutputInterval: " + convergenceProgressOutputInterval + "\n");
                output.writeBytes("-------------------------------------------------------------\n");
                output.writeBytes("\n");
                output.writeBytes("===================Convergence Progress======================\n");
            } else if (realStep >= 1 && realStep % convergenceProgressOutputInterval == 0) {
                // output learning progress
                double sumDelta = Double.parseDouble(map.get(SUM_DELTA_AGG));

                output.writeBytes("superstep = " + realStep + "\tsumDelta = " + sumDelta + "\n");
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
