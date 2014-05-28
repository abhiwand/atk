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

package com.intel.giraph.algorithms.gbp;

import com.intel.giraph.io.GaussianDistWritable;
import com.intel.giraph.io.MessageData4GBPWritable;
import com.intel.giraph.io.VertexData4GBPWritable;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * Gaussian belief propagation on MRF
 */
@Algorithm(
    name = "Gaussian belief propagation on MRF"
)
public class GaussianBeliefPropagationComputation extends BasicComputation<LongWritable, VertexData4GBPWritable,
    DoubleWritable, MessageData4GBPWritable> {

    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "gbp.maxSupersteps";
    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "gbp.convergenceThreshold";
    /** Custom argument for checking bi-directional edge or not (default: false) */
    public static final String BIDIRECTIONAL_CHECK = "gbp.bidirectionalCheck";

    /** Aggregator name for sum of delta for convergence monitoring */
    private static final String SUM_DELTA = "delta";
    /** Average delta value on validation data of previous super step for convergence monitoring */
    private static final String PREV_AVG_DELTA = "prev_avg_delta";

    /** Number of super steps */
    private int maxSupersteps = 10;
    /** Turning on/off bi-directional edge check */
    private boolean bidirectionalCheck = false;

    @Override
    public void preSuperstep() {
        // set custom parameters
        maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
        bidirectionalCheck = getConf().getBoolean(BIDIRECTIONAL_CHECK, false);
    }

    /**
     * Initialize vertex
     *
     * @param vertex of the graph
     */
    private void initializeVertex(Vertex<LongWritable, VertexData4GBPWritable, DoubleWritable> vertex) {
        // normalize prior and posterior
        GaussianDistWritable prior = vertex.getValue().getPrior();
        GaussianDistWritable posterior = vertex.getValue().getPosterior();
        double pii = prior.getPrecision();
        if (pii == 0d) {
            throw new IllegalArgumentException("Vertex: " + vertex.getId() +
                " has precision value 0, which is invalid.");
        }
        double uii = prior.getMean() / pii;
        prior.setMean(uii);
        prior.setPrecision(pii);
        posterior.set(prior);

        // calculate messages
        MessageData4GBPWritable newMessage = new MessageData4GBPWritable();
        newMessage.setId(vertex.getId().get());

        GaussianDistWritable gauss = new GaussianDistWritable();
        for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
            double weight = edge.getValue().get();
            if (weight == 0d) {
                throw new IllegalArgumentException("Vertex: " + vertex.getId() +
                    " has an edge with weight value 0. This edge should be removed from the graph.");
            }
            double uij = pii * uii / weight;
            double pij = - weight * weight / pii;
            gauss.setMean(uij);
            gauss.setPrecision(pij);
            // send out messages
            newMessage.setGauss(gauss);
            sendMessage(edge.getTargetVertexId(), newMessage);
        }
    }

    @Override
    public void compute(Vertex<LongWritable, VertexData4GBPWritable, DoubleWritable> vertex,
        Iterable<MessageData4GBPWritable> messages) throws IOException {

        long step = getSuperstep();
        if (step == 0) {
            initializeVertex(vertex);
            vertex.voteToHalt();
            return;
        }

        // collect messages sent to this vertex
        HashMap<Long, List<Double>> map = new HashMap<Long, List<Double>>();
        for (MessageData4GBPWritable message : messages) {
            List<Double> paras = new ArrayList<Double>();
            paras.add(message.getGauss().getMean());
            paras.add(message.getGauss().getPrecision());
            map.put(message.getId(), paras);
        }
        if (bidirectionalCheck) {
            if (map.size() != vertex.getNumEdges()) {
                throw new IllegalArgumentException(String.format("Vertex ID %d: Number of received messages (%d)" +
                    " isn't equal to number of edges (%d).", vertex.getId().get(),
                    map.size(), vertex.getNumEdges()));
            }
        }

        // update posterior according to prior and messages
        VertexData4GBPWritable vertexValue = vertex.getValue();
        GaussianDistWritable prior = vertexValue.getPrior();
        // sum of prior and messages
        double uii = prior.getMean();
        double pii = prior.getPrecision();
        double sum4Mean = uii * pii;
        double sum4Precision = pii;
        for (MessageData4GBPWritable message : messages) {
            double uki = message.getGauss().getMean();
            double pki = message.getGauss().getPrecision();
            sum4Mean += uki * pki;
            sum4Precision += pki;
        }

        GaussianDistWritable posterior = new GaussianDistWritable();
        double u = sum4Mean / sum4Precision;
        double p = sum4Precision;
        posterior.setMean(u);
        posterior.setPrecision(p);
        // aggregate deltas for convergence monitoring
        GaussianDistWritable oldPosterior = vertexValue.getPosterior();
        double u0 = oldPosterior.getMean();
        double delta = Math.abs(u - u0);
        aggregate(SUM_DELTA, new DoubleWritable(delta));
        // update posterior
        vertexValue.setPosterior(posterior);

        if (step < maxSupersteps) {
            MessageData4GBPWritable newMessage = new MessageData4GBPWritable();
            newMessage.setId(vertex.getId().get());
            // update belief
            GaussianDistWritable gauss = new GaussianDistWritable();
            for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
                double weight = edge.getValue().get();
                long id = edge.getTargetVertexId().get();
                double tempMean = sum4Mean;
                double tempPrecision = sum4Precision;
                if (map.containsKey(id)) {
                    tempPrecision = sum4Precision - map.get(id).get(1);
                    tempMean = sum4Mean - map.get(id).get(0) * map.get(id).get(1);
                } else {
                    throw new IllegalArgumentException(String.format("Vertex ID %d: A message is mis-matched",
                        vertex.getId().get()));
                }
                // send out messages
                double uij = tempMean / weight;
                double pij = - weight * weight / tempPrecision;
                gauss.setMean(uij);
                gauss.setPrecision(pij);
                newMessage.setGauss(gauss);
                sendMessage(edge.getTargetVertexId(), newMessage);
            }
        }

        vertex.voteToHalt();
    }

    /**
     * Master compute associated with {@link GaussianBeliefPropagationComputation}. It registers required aggregators.
     */
    public static class GaussianBeliefPropagationMasterCompute extends DefaultMasterCompute {
        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerAggregator(SUM_DELTA, DoubleSumAggregator.class);
        }

        @Override
        public void compute() {
            long step = getSuperstep();
            if (step <= 0) {
                return;
            }

            if (step != 1) {
                // calculate average delta
                DoubleWritable sumDelta = getAggregatedValue(SUM_DELTA);
                double avgDelta = sumDelta.get() / getTotalNumVertices();
                sumDelta.set(avgDelta);
                // evaluate convergence condition
                float threshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                float prevAvgDelta = getConf().getFloat(PREV_AVG_DELTA, 0f);
                if (Math.abs(prevAvgDelta - avgDelta) < threshold) {
                    getConf().setInt(MAX_SUPERSTEPS, (int) step);
                }
                getConf().setFloat(PREV_AVG_DELTA, (float) avgDelta);
            }
        }
    }

    /**
     * This is an aggregator writer for gbp, which after each super step will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class GaussianBeliefPropagationAggregatorWriter extends DefaultImmutableClassesGiraphConfigurable
        implements AggregatorWriter {
        /** Name of the file we wrote to */
        private static String FILENAME;
        /** Saved output stream to write to */
        private FSDataOutputStream output;
        /** Last superstep number */
        private long lastStep = -1L;

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
            HashMap<String, String> map = new HashMap<String, String>();
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
                output.writeBytes("======GBP Configuration======\n");
                output.writeBytes(String.format("maxSupersteps: %d%n", maxSupersteps));
                output.writeBytes(String.format("convergenceThreshold: %f%n", convergenceThreshold));
                output.writeBytes(String.format("bidirectionalCheck: %b%n", bidirectionalCheck));
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
    }

}
