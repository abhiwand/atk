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

package com.intel.taproot.giraph.algorithms.gbp;

import com.intel.taproot.giraph.io.GaussianDistWritable;
import com.intel.taproot.giraph.io.MessageData4GBPWritable;
import com.intel.taproot.giraph.io.VertexData4GBPWritable;
import com.intel.taproot.giraph.io.EdgeData4GBPWritable;
import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * Gaussian belief propagation on MRF
 * Algorithm is described in the paper:
 * "Fixing convergence of Gaussian belief propagation" by
 * J. K. Johnson, D. Bickson and D. Dolev
 * In ISIT 2009
 * http://arxiv.org/abs/0901.4192
 */
@Algorithm(
    name = "Gaussian belief propagation on MRF"
)
public class GaussianBeliefPropagationComputation extends BasicComputation<LongWritable, VertexData4GBPWritable,
    EdgeData4GBPWritable, MessageData4GBPWritable> {

    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "gbp.maxSupersteps";
    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "gbp.convergenceThreshold";
    /** Custom argument for checking bi-directional edge or not (default: false) */
    public static final String BIDIRECTIONAL_CHECK = "gbp.bidirectionalCheck";
    /** enable outer loop for convergence */
    public static final String OUTER_LOOP = "gbp.outerLoop";


    /** Aggregator name for sum of delta for convergence monitoring */
    private static final String SUM_DELTA = "delta";
    /** Average delta value on validation data of previous super step for convergence monitoring */
    private static final String PREV_AVG_DELTA = "prev_avg_delta";
    /** Aggregator name for max(sum(abs(A)) */
    private static final String MAX_SUM = "max_sum";
    /** Aggregator name for max(diag(A))) */
    private static final String MAX_DIAG = "max_diagonal";
    /** GBP execution stage */
    private static final String STAGE = "gbp_stage";
    /** diagonal loading value */
    private static final String DIAG_LOADING = "diag_oading";
    /** enable outer loop for convergence */
    private static final String FIRST_OUTER = "first_outer";

    /** Number of super steps */
    private int maxSupersteps = 10;
    /** Turning on/off bi-directional edge check */
    private boolean bidirectionalCheck = false;
    /** Execution stage */
    private int stage = 0;
    /** whether it is the first iteration of outer loop */
    private boolean firstOuter = true;
    /** current super step */
    private int step = 0;
    /** whether outer loop is enabled */
    private boolean outerLoop = true;

    @Override
    public void preSuperstep() {
        // set custom parameters
        maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
        bidirectionalCheck = getConf().getBoolean(BIDIRECTIONAL_CHECK, false);
        stage = getConf().getInt(STAGE, 0);
        firstOuter = getConf().getBoolean(FIRST_OUTER, true);
        step = (int) getSuperstep();
        outerLoop = getConf().getBoolean(OUTER_LOOP, true);
    }

    /**
     * Calculate diagonal Loading
     *
     * @param vertex of the graph
     */
    private void diagonalLoading(Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex) {
        //initialize posterior
        GaussianDistWritable prior = vertex.getValue().getPrior();
        GaussianDistWritable posterior = vertex.getValue().getPosterior();
        posterior.set(prior);
        double sum = vertex.getValue().getPrior().getPrecision();
        aggregate(MAX_DIAG, new DoubleWritable(sum));
        sum = Math.abs(sum);
        for (Edge<LongWritable, EdgeData4GBPWritable> edge : vertex.getEdges()) {
            sum += Math.abs(edge.getValue().getReverseWeight());
        }
        aggregate(MAX_SUM, new DoubleWritable(sum));
    }

    /**
     * Update diagonal value
     *
     * @param vertex of the graph
     */
    private void updateDiagonal(Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex) {
        double diagLoading = getConf().getDouble(DIAG_LOADING, 0d);
        GaussianDistWritable intermediate = vertex.getValue().getIntermediate();
        double oldValue = intermediate.getPrecision();
        intermediate.setPrecision(oldValue + diagLoading);
    }

    /**
     * Send messages for calculating A*xj
     *
     * @param vertex of the graph
     */
    private void sendMeanMsg(Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex) {
        GaussianDistWritable posterior = vertex.getValue().getPosterior();
        double meanIi = posterior.getMean();
        vertex.getValue().setPrevMean(meanIi);

        // calculate messages
        MessageData4GBPWritable newMessage = new MessageData4GBPWritable();
        newMessage.setId(vertex.getId().get());
        GaussianDistWritable gauss = new GaussianDistWritable();
        for (Edge<LongWritable, EdgeData4GBPWritable> edge : vertex.getEdges()) {
            double precisionIj = edge.getValue().getReverseWeight();
            gauss.setMean(meanIi);
            gauss.setPrecision(precisionIj);
            // send out messages
            newMessage.setGauss(gauss);
            sendMessage(edge.getTargetVertexId(), newMessage);
        }
    }

    /**
     * Update mean to b = b - A*xj
     *
     * @param vertex of the graph
     * @param messages to out edges
     */
    private void updateMean(Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex,
                            Iterable<MessageData4GBPWritable> messages) throws IOException {
        GaussianDistWritable prior = vertex.getValue().getPrior();
        GaussianDistWritable posterior = vertex.getValue().getPosterior();
        GaussianDistWritable intermediate = vertex.getValue().getIntermediate();
        double sum = prior.getMean() - posterior.getMean() * prior.getPrecision();
        for (MessageData4GBPWritable message : messages) {
            double meanKi = message.getGauss().getMean();
            double precisionKi = message.getGauss().getPrecision();
            sum -= precisionKi * meanKi;
        }
        intermediate.setMean(sum);
    }

    /**
     * Initialize vertex
     *
     * @param vertex of the graph
     */
    private void initializeInnerLoop(Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex) {
        // normalize prior and posterior
        GaussianDistWritable intermediate = vertex.getValue().getIntermediate();
        GaussianDistWritable posterior = vertex.getValue().getPosterior();
        double precisionIi = intermediate.getPrecision();
        double meanIi = intermediate.getMean();
        posterior.setMean(meanIi);
        posterior.setPrecision(precisionIi);
        // calculate messages
        MessageData4GBPWritable newMessage = new MessageData4GBPWritable();
        newMessage.setId(vertex.getId().get());

        GaussianDistWritable gauss = new GaussianDistWritable();
        for (Edge<LongWritable, EdgeData4GBPWritable> edge : vertex.getEdges()) {
            double weightIj = edge.getValue().getWeight();
            double weightJi = edge.getValue().getReverseWeight();

            if (weightIj == 0d && weightJi == 0d) {
                throw new IllegalArgumentException("Vertex: " + vertex.getId() +
                    " has empty edge weights on both directions to vertex " + edge.getTargetVertexId());
            }

            if (precisionIi == 0d) {
                throw new IllegalArgumentException("Vertex: " + vertex.getId() + " has zero diagonal value!");
            }

            double meanIj = - weightJi * meanIi / precisionIi;
            double precisionIj = - weightIj * weightJi / precisionIi;
            gauss.setMean(meanIj);
            gauss.setPrecision(precisionIj);
            // send out messages
            newMessage.setGauss(gauss);
            sendMessage(edge.getTargetVertexId(), newMessage);
        }
    }


    /**
     * Inner Loop to calculate [direc,J,r1] = asynch_GBP(Minc, b - A*xj, max_iter, epsilon)
     *
     * @param vertex of the graph
     * @param messages to out edges
     */
    private void innerLoop(Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex,
                            Iterable<MessageData4GBPWritable> messages) throws IOException {
        // update posterior according to prior and messages
        VertexData4GBPWritable vertexValue = vertex.getValue();
        GaussianDistWritable intermediate = vertexValue.getIntermediate();
        // sum of prior and messages
        double sum4Mean = intermediate.getMean();
        double sum4Precision = intermediate.getPrecision();
        // collect messages sent to this vertex
        HashMap<Long, List<Double>> map = new HashMap<Long, List<Double>>();
        for (MessageData4GBPWritable message : messages) {
            double meanKi = message.getGauss().getMean();
            double precisionKi = message.getGauss().getPrecision();
            List<Double> paras = new ArrayList<>();
            paras.add(meanKi);
            paras.add(precisionKi);
            map.put(message.getId(), paras);

            sum4Mean += meanKi;
            sum4Precision += precisionKi;
        }

        if (bidirectionalCheck) {
            if (map.size() != vertex.getNumEdges()) {
                throw new IllegalArgumentException(String.format("Vertex ID %d: Number of received messages (%d)" +
                        " isn't equal to number of edges (%d).", vertex.getId().get(),
                    map.size(), vertex.getNumEdges()));
            }
        }

        GaussianDistWritable posterior = new GaussianDistWritable();
        double precision = 1.0 / sum4Precision;
        posterior.setPrecision(precision);
        double mean = sum4Mean * precision;
        posterior.setMean(mean);
        // aggregate deltas for convergence monitoring
        GaussianDistWritable oldPosterior = vertexValue.getPosterior();
        double oldMean = oldPosterior.getMean();
        double delta = Math.abs(mean - oldMean);
        aggregate(SUM_DELTA, new DoubleWritable(delta));
        // update posterior
        vertexValue.setPosterior(posterior);

        if (maxSupersteps > getSuperstep()) {
            MessageData4GBPWritable newMessage = new MessageData4GBPWritable();
            newMessage.setId(vertex.getId().get());
            // update belief
            GaussianDistWritable gauss = new GaussianDistWritable();
            for (Edge<LongWritable, EdgeData4GBPWritable> edge : vertex.getEdges()) {
                double weightIj = edge.getValue().getWeight();
                double weightJi = edge.getValue().getReverseWeight();
                long id = edge.getTargetVertexId().get();
                double tempMean = sum4Mean;
                double tempPrecision = sum4Precision;
                if (map.containsKey(id)) {
                    tempPrecision = sum4Precision - map.get(id).get(1);
                    tempMean = sum4Mean - map.get(id).get(0);
                }

                if (tempPrecision == 0d) {
                    throw new IllegalArgumentException("Vertex: " + vertex.getId() +
                        " get zero precision to target " + edge.getTargetVertexId());
                }

                // send out messages
                double meanIj = - weightJi * tempMean / tempPrecision;
                double precisionIj = - weightIj * weightJi / tempPrecision;
                gauss.setMean(meanIj);
                gauss.setPrecision(precisionIj);
                newMessage.setGauss(gauss);
                sendMessage(edge.getTargetVertexId(), newMessage);
            }
        }
    }

    /**
     * Update posterior mean
     *
     * @param vertex of the graph
     */
    private void updatePosteirorMean(Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex) {
        GaussianDistWritable posterior = vertex.getValue().getPosterior();
        double meanIi = vertex.getValue().getPrevMean();
        double newMeanIi = meanIi + posterior.getMean();
        double delta;
        //in first outer iteration old_xj=0
        //delta is norm(old_xj - xj) = norm(posterior.getMean())
        if (firstOuter) {
            delta = Math.abs(newMeanIi);
            getConf().setBoolean(FIRST_OUTER, false);
        } else {
            delta = Math.abs(posterior.getMean());
        }
        aggregate(SUM_DELTA, new DoubleWritable(delta));
        posterior.setMean(newMeanIi);
    }

    @Override
    public void compute(Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex,
        Iterable<MessageData4GBPWritable> messages) throws IOException {
        if (step == 0) {
            if (!outerLoop) {
                initializeInnerLoop(vertex);
                getConf().setInt(STAGE, 5);
                vertex.voteToHalt();
            } else {
                diagonalLoading(vertex);
            }
            return;
        }

        if (outerLoop && maxSupersteps == step) {
            updatePosteirorMean(vertex);
            vertex.voteToHalt();
            return;
        }

        switch(stage) {
        case 1:
            updateDiagonal(vertex);
            getConf().setInt(STAGE, 2);
            return;
        case 2:
            sendMeanMsg(vertex);
            getConf().setInt(STAGE, 3);
            vertex.voteToHalt();
            return;
        case 3:
            updateMean(vertex, messages);
            getConf().setInt(STAGE, 4);
            return;
        case 4:
            initializeInnerLoop(vertex);
            getConf().setInt(STAGE, 5);
            vertex.voteToHalt();
            return;
        case 5:
        case 6:
            innerLoop(vertex, messages);
            getConf().setInt(STAGE, 6);
            vertex.voteToHalt();
            return;
        case 7:
            updatePosteirorMean(vertex);
            getConf().setInt(STAGE, 2);
            return;
        case 8:
            vertex.voteToHalt();
            return;
        default:
            break;
        }
    }

    /**
     * Master compute associated with {@link GaussianBeliefPropagationComputation}. It registers required aggregators.
     */
    public static class GaussianBeliefPropagationMasterCompute extends DefaultMasterCompute {
        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerAggregator(SUM_DELTA, DoubleSumAggregator.class);
            registerAggregator(MAX_SUM, DoubleMaxAggregator.class);
            registerAggregator(MAX_DIAG, DoubleMaxAggregator.class);
        }

        /**
         * Examine diagLoading result and update control flow based on the result
         */
        private void diagLoadingResult() {
            DoubleWritable maxSumValue = getAggregatedValue(MAX_SUM);
            double maxSum = maxSumValue.get();
            DoubleWritable maxDiagValue = getAggregatedValue(MAX_DIAG);
            double maxDiag = maxDiagValue.get();
            double diagLoading = maxSum - maxDiag;

            if (diagLoading == 0d) {
                getConf().setBoolean(OUTER_LOOP, false);
                getConf().setInt(STAGE, 4);
            } else {
                getConf().setBoolean(OUTER_LOOP, true);
                getConf().setInt(STAGE, 1);
                getConf().setDouble(DIAG_LOADING, diagLoading);
            }
        }

        /**
         * Evaluate convergence conditions and update control flow based on the result
         *
         * @param stage of type int
         */
        private void evaluateConvergence(int stage) {
            // calculate average delta
            DoubleWritable sumDelta = getAggregatedValue(SUM_DELTA);
            double avgDelta = sumDelta.get() / getTotalNumVertices();
            float threshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
            float prevAvgDelta = getConf().getFloat(PREV_AVG_DELTA, 0f);
            boolean outerLoop = getConf().getBoolean(OUTER_LOOP, false);
            //evaluate convergence condition
            double normalizedDelta =  Math.abs(prevAvgDelta - avgDelta);
            double delta = Math.abs(avgDelta);
            if ((stage == 6) && outerLoop && (normalizedDelta < threshold)) {
                getConf().setInt(STAGE, 7);
            }

            if (((stage == 6) && !outerLoop && (normalizedDelta < threshold)) ||
                ((stage == 2) && (delta < threshold))) {
                getConf().setInt(STAGE, 8);
            }
            sumDelta.set(avgDelta);
            getConf().setFloat(PREV_AVG_DELTA, (float) avgDelta);
        }

        @Override
        public void compute() {
            long step = getSuperstep();
            int stage = getConf().getInt(STAGE, 0);
            boolean outerLoop = getConf().getBoolean(OUTER_LOOP, false);
            if (step < 0) {
                return;
            }

            if (step == 1 && outerLoop) {
                diagLoadingResult();
            } else if ((step > 1 && !outerLoop) ||
                ((stage == 6 || stage == 2) && step > 5))  {
                evaluateConvergence(stage);
            }
        }
    }

    /**
     * This is an aggregator writer for gbp, which after each super step will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class GaussianBeliefPropagationAggregatorWriter implements AggregatorWriter {
        /** Name of the file we wrote to */
        private static String FILENAME;
        /** Saved output stream to write to */
        private FSDataOutputStream output;
        /** Last superstep number */
        private long lastStep = -1L;

        private ImmutableClassesGiraphConfiguration conf;

        public static String getFilename() {
            return FILENAME;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void initialize(Context context, long applicationAttempt) throws IOException {
            setFilename(applicationAttempt);
            String outputDir = context.getConfiguration().get("mapred.output.dir");
            Path p = new Path(outputDir + "/" + FILENAME);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            if (fs.exists(p)) {
                fs.delete(p, true);
            }
            output = fs.create(p, true);
        }

        /**
         * Set filename written to
         *
         * @param applicationAttempt of type long
         */
        private static void setFilename(long applicationAttempt) {
            FILENAME = "gbp-learning-report_" + applicationAttempt;
        }

        @Override
        public void writeAggregator(Iterable<Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {
            long realStep = lastStep;

            // collect aggregator data
            HashMap<String, String> map = new HashMap<>();
            for (Entry<String, Writable> entry : aggregatorMap) {
                map.put(entry.getKey(), entry.getValue().toString());
            }

            if (realStep == 0) {
                // output graph statistics
                output.writeBytes("======Graph Statistics======\n");
                output.writeBytes(String.format("Number of vertices: %d%n",
                    GiraphStats.getInstance().getVertices().getValue()));
                output.writeBytes(String.format("Number of edges: %d%n",
                    GiraphStats.getInstance().getEdges().getValue()));
                output.writeBytes("\n");
                // output GBP configuration
                int maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
                float convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                boolean bidirectionalCheck = getConf().getBoolean(BIDIRECTIONAL_CHECK, false);
                boolean outerLoop = getConf().getBoolean(OUTER_LOOP, true);
                output.writeBytes("======GBP Configuration======\n");
                output.writeBytes(String.format("maxSupersteps: %d%n", maxSupersteps));
                output.writeBytes(String.format("convergenceThreshold: %f%n", convergenceThreshold));
                output.writeBytes(String.format("bidirectionalCheck: %b%n", bidirectionalCheck));
                output.writeBytes(String.format("outerLoop: %b%n", outerLoop));
                output.writeBytes("\n");
                output.writeBytes("======Learning Progress======\n");
            } else if (realStep > 0) {
                // output learning progress
                double avgDelta = Double.parseDouble(map.get(SUM_DELTA));
                output.writeBytes(String.format("superstep = %d%c", realStep, '\t'));
                output.writeBytes(String.format("avgDelta = %f%n", avgDelta, '\t'));
            }
            output.flush();
            lastStep =  superstep;
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
