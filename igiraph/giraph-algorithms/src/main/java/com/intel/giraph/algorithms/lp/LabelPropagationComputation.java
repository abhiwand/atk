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

package com.intel.giraph.algorithms.lp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.mahout.math.Vector;

import com.intel.giraph.io.VertexData4LPWritable;
import com.intel.mahout.math.IdWithVectorWritable;

/**
 * Label Propagation on Gaussian Random Fields
 * The algorithm presented in:
 * X. Zhu and Z. Ghahramani. Learning from labeled and unlabeled data with
 * label propagation. Technical Report CMU-CALD-02-107, CMU, 2002.
 */
@Algorithm(
    name = "Label Propagation on Gaussian Random Fields"
)
public class LabelPropagationComputation extends BasicComputation<LongWritable, VertexData4LPWritable,
    DoubleWritable, IdWithVectorWritable> {
    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "lp.maxSupersteps";
    /**
     * Custom argument for tradeoff parameter: lambda
     * f = (1-lambda)*Pf + lambda*h
     */
    public static final String LAMBDA = "lp.lambda";
    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "lp.convergenceThreshold";
    /**
     * Custom argument for the anchor threshold [0, 1]
     * the vertices whose normalized values are greater than
     * this threshold will not be updated.
     */
    public static final String ANCHOR_THRESHOLD = "lp.anchorThreshold";
    /** Custom argument for checking bi-directional edge or not (default: false) */
    public static final String BIDIRECTIONAL_CHECK = "lp.bidirectionalCheck";
    /** Number of vertices */
    private static final String NUM_VERTICES = "num_vertices";
    /** Number of edges */
    private static final String NUM_EDGES = "num_edges";
    /** Aggregator name for sum of cost at each super step */
    private static String SUM_COST = "sum_cost";
    /** Cost of previous super step for convergence monitoring */
    private static String PREV_COST = "prev_cost";

    /** Number of super steps */
    private int maxSupersteps = 10;
    /** The tradeoff parameter between prior and posterior */
    private float lambda = 0f;
    /** The anchor threshold controlling if updating a vertex */
    private float anchorThreshold = 1f;
    /** Turning on/off bi-directional edge check */
    private boolean bidirectionalCheck = false;

    @Override
    public void preSuperstep() {
        // Set custom parameters
        maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
        lambda = getConf().getFloat(LAMBDA, 0f);
        if (lambda < 0 || lambda > 1) {
            throw new IllegalArgumentException("Tradeoff parameter lambda should be in the range of [0, 1].");
        }
        anchorThreshold = getConf().getFloat(ANCHOR_THRESHOLD, 1f);
        bidirectionalCheck = getConf().getBoolean(BIDIRECTIONAL_CHECK, false);
    }

    /**
     * initialize vertex and edges
     *
     * @param vertex of the graph
     */
    private void initializeVertexEdges(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex) {
        VertexData4LPWritable vertexValue = vertex.getValue();
        // normalize prior and initialize posterior
        Vector prior = vertexValue.getPriorVector();
        if (prior.minValue() < 0d) {
            throw new IllegalArgumentException("Vertex ID: " + vertex.getId() + " has negative prior value.");
        }
        prior = prior.normalize(1d);
        vertexValue.setPriorVector(prior);
        vertexValue.setPosteriorVector(prior.clone());
        // normalize edge weight
        double degree = 0d;
        for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
            double weight = edge.getValue().get();
            if (weight <= 0d) {
                throw new IllegalArgumentException("Vertex ID: " + vertex.getId() +
                    " has an edge with negative or zero weight value.");
            }
            degree += weight;
        }
        for (Edge<LongWritable, DoubleWritable> edge : vertex.getMutableEdges()) {
            edge.getValue().set(edge.getValue().get() / degree);
        }
        vertexValue.setDegree(degree);
        // send out messages
        IdWithVectorWritable newMessage = new IdWithVectorWritable(vertex.getId().get(), prior);
        sendMessageToAllEdges(vertex, newMessage);
    }

    @Override
    public void compute(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex,
        Iterable<IdWithVectorWritable> messages) throws IOException {
        long step = getSuperstep();
        if (step == 0) {
            initializeVertexEdges(vertex);
            vertex.voteToHalt();
            return;
        }

        if (step <= maxSupersteps) {
            VertexData4LPWritable vertexValue = vertex.getValue();
            Vector prior = vertexValue.getPriorVector();
            Vector posterior = vertexValue.getPosteriorVector();
            double degree = vertexValue.getDegree();

            // collect messages sent to this vertex
            HashMap<Long, Vector> map = new HashMap<Long, Vector>();
            for (IdWithVectorWritable message : messages) {
                map.put(message.getData(), message.getVector());
            }
            if (bidirectionalCheck) {
                if (map.size() != vertex.getNumEdges()) {
                    throw new IllegalArgumentException(String.format("Vertex %d: Number of received messages (%d) " +
                        " isn't equal to number of edges (%d).", vertex.getId(), map.size(), vertex.getNumEdges()));
                }
            }

            // Update belief and calculate cost
            double hi = prior.getQuick(0);
            double fi = posterior.getQuick(0);
            double crossSum = 0d;
            Vector oldBelief = posterior;
            Vector belief = oldBelief.clone().assign(0d);
            for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
                double weight = edge.getValue().get();
                long id = edge.getTargetVertexId().get();
                if (map.containsKey(id)) {
                    Vector tempVector = map.get(id);
                    belief = belief.plus(tempVector.times(weight));
                    double fj = tempVector.getQuick(0);
                    crossSum += weight * fi * fj;
                }
            }
            double cost = degree * ((1 - lambda) * (fi * fi - crossSum) + 0.5 * lambda * (fi - hi) * (fi - hi));
            aggregate(SUM_COST, new DoubleWritable(cost));

            // Update posterior if this isn't an anchor vertex
            if (prior.maxValue() < anchorThreshold) {
                belief = belief.times(1 - lambda).plus(prior.times(lambda));
                vertexValue.setPosteriorVector(belief);
            }

            if (step != maxSupersteps) {
                // Send out messages
                IdWithVectorWritable newMessage = new IdWithVectorWritable(vertex.getId().get(),
                    vertexValue.getPosteriorVector());
                sendMessageToAllEdges(vertex, newMessage);
            }
        }

        vertex.voteToHalt();
    }

    /**
     * Master compute associated with {@link LabelPropagationComputation}. It registers required aggregators.
     */
    public static class LabelPropagationMasterCompute extends DefaultMasterCompute {

        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerAggregator(SUM_COST, DoubleSumAggregator.class);
        }

        @Override
        public void compute() {
            long step = getSuperstep();
            if (step <= 0) {
                return;
            }
            if (step == 1) {
                // collect graph statistics
                long numVertices = getTotalNumVertices();
                long numEdges = getTotalNumEdges();
                getConf().setLong(NUM_VERTICES, numVertices);
                getConf().setLong(NUM_EDGES, numEdges);
            } else {
                // evaluate convergence condition
                float threshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                float prevCost = getConf().getFloat(PREV_COST, 0f);
                DoubleWritable sumCost = getAggregatedValue(SUM_COST);
                double cost = sumCost.get() / getTotalNumVertices();
                sumCost.set(cost);
                if (Math.abs(prevCost - cost) < threshold) {
                    haltComputation();
                }
                getConf().setFloat(PREV_COST, (float) cost);
            }
        }
    }

    /**
     * This is an aggregator writer for label propagation, which after each super step will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class LabelPropagationAggregatorWriter extends DefaultImmutableClassesGiraphConfigurable
        implements AggregatorWriter {
        /** Name of the file we wrote to */
        private static String FILENAME;
        /** Saved output stream to write to */
        private FSDataOutputStream output;
        /** Last superstep number */
        private long lastStep = 0L;

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
            FILENAME = "lp-learning-report_" + applicationAttempt;
        }

        @Override
        public void writeAggregator(Iterable<Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {
            long realStep = lastStep;

            if (realStep == 1) {
                // output graph statistics
                long numVertices = getConf().getLong(NUM_VERTICES, 0L);
                long numEdges = getConf().getLong(NUM_EDGES, 0L);
                output.writeBytes("======Graph Statistics======\n");
                output.writeBytes(String.format("Number of vertices: %d%n", numVertices));
                output.writeBytes(String.format("Number of edges: %d%n", numEdges));
                output.writeBytes("\n");
                // output LP configuration
                float lambda = getConf().getFloat(LAMBDA, 0f);
                float anchorThreshold = getConf().getFloat(ANCHOR_THRESHOLD, 1f);
                float convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                int maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
                boolean bidirectionalCheck = getConf().getBoolean(BIDIRECTIONAL_CHECK, false);
                output.writeBytes("======LP Configuration======\n");
                output.writeBytes(String.format("lambda: %f%n", lambda));
                output.writeBytes(String.format("anchorThreshold: %f%n", anchorThreshold));
                output.writeBytes(String.format("convergenceThreshold: %f%n", convergenceThreshold));
                output.writeBytes(String.format("maxSupersteps: %d%n", maxSupersteps));
                output.writeBytes(String.format("bidirectionalCheck: %b%n", bidirectionalCheck));
                output.writeBytes("\n");
                output.writeBytes("======Learning Progress======\n");
            }
            if (realStep > 0) {
                // collect aggregator data
                HashMap<String, String> map = new HashMap<String, String>();
                for (Entry<String, Writable> entry : aggregatorMap) {
                    map.put(entry.getKey(), entry.getValue().toString());
                }
                // output learning progress
                output.writeBytes(String.format("superstep=%d%c", realStep, '\t'));
                double cost = Double.parseDouble(map.get(SUM_COST));
                output.writeBytes(String.format("cost=%f%n", cost));
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
