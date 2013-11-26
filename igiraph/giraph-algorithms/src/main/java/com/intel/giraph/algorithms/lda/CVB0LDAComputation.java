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

package com.intel.giraph.algorithms.lda;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

import com.intel.giraph.io.VertexData4LDAWritable;
import com.intel.giraph.io.VertexData4LDAWritable.VertexType;
import com.intel.giraph.aggregators.VectorSumAggregator;
import com.intel.mahout.math.IdWithVectorWritable;
import com.intel.mahout.math.DoubleWithVectorWritable;

/**
 * CVB0 Latent Dirichlet Allocation for Topic Modelling
 * The algorithm presented in
 * Y.W. Teh, D. Newman, and M. Welling, A Collapsed Variational Bayesian
 * Inference Algorithm for Latent Dirichlet Allocation, NIPS 19, 2007.
 */
@Algorithm(
    name = "CVB0 Latent Dirichlet Allocation"
)
public class CVB0LDAComputation extends BasicComputation<LongWritable, VertexData4LDAWritable,
    DoubleWithVectorWritable, IdWithVectorWritable> {
    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "lda.maxSupersteps";
    /** Custom argument for number of topics */
    public static final String NUM_TOPICS = "lda.numTopics";
    /** Custom argument for document-topic smoothing parameter: alpha */
    public static final String ALPHA = "lda.alpha";
    /** Custom argument for term-topic smoothing parameter: beta */
    public static final String BETA = "lda.beta";
    /** Custom argument for convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "lda.convergenceThreshold";
    /** Custom argument for maximum edge weight value */
    public static final String MAX_VAL = "lda.maxVal";
    /** Custom argument for minimum edge weight value */
    public static final String MIN_VAL = "lda.minVal";
    /** Custom argument for turning on/off cost evaluation  */
    public static final String COST_EVAL = "lda.evaluateCost";

    /** Aggregator name for number of document-vertices */
    private static String SUM_DOC_VERTEX_COUNT = "num_doc_vertices";
    /** Aggregator name for number of word-vertices */
    private static String SUM_WORD_VERTEX_COUNT = "num_word_vertices";
    /** Aggregator name for number of word occurrences */
    private static String SUM_OCCURRENCE_COUNT = "num_occurrences";
    /** Aggregator name for sum of word-vertex values, the nk in LDA */
    private static String SUM_WORD_VERTEX_VALUE = "nk";
    /** Aggregator name for max of delta at each super step */
    private static String SUM_COST = "sum_cost";
    /** Aggregator name for max of delta at each super step */
    private static String MAX_DELTA = "max_delta";
    /** Number of edges */
    private static String NUM_EDGES = "num_edges";
    /** Max delta value of previous super step for convergence monitoring */
    private static String PREV_MAX_DELTA = "prev_max_delta";

    /** Number of super steps */
    private int maxSupersteps = 20;
    /** Number of topics */
    private int numTopics = 10;
    /** Document-topic smoothing parameter: alpha */
    private float alpha = 0.1f;
    /** Term-topic smoothing parameter: beta */
    private float beta = 0.1f;
    /** Maximum edge weight value */
    private float maxVal = Float.POSITIVE_INFINITY;
    /** Minimum edge weight value */
    private float minVal = Float.NEGATIVE_INFINITY;
    /** Turning on/off cost evaluation */
    private boolean costEval = false;
    /** Number of words in vocabulary */
    private long numWords = 0;
    /** Sum of word-vertex values */
    private Vector nk = null;

    @Override
    public void preSuperstep() {
        // Set custom parameters
        maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 20);
        numTopics = getConf().getInt(NUM_TOPICS, 10);
        if (numTopics < 1) {
            throw new IllegalArgumentException("Number of topics (K) should be > 0.");
        }
        alpha = getConf().getFloat(ALPHA, 0.1f);
        beta = getConf().getFloat(BETA, 0.1f);
        if (alpha <= 0 || beta <= 0) {
            throw new IllegalArgumentException("alpha and beta should be > 0.");
        }
        maxVal = getConf().getFloat(MAX_VAL, Float.POSITIVE_INFINITY);
        minVal = getConf().getFloat(MIN_VAL, Float.NEGATIVE_INFINITY);
        costEval = getConf().getBoolean(COST_EVAL, false);
        numWords = this.<LongWritable>getAggregatedValue(SUM_WORD_VERTEX_COUNT).get();
        nk = this.<VectorWritable>getAggregatedValue(SUM_WORD_VERTEX_VALUE).get().clone();
    }

    /**
     * initialize vertex/edges, collect graph statistics and send out messages
     *
     * @param vertex of the graph
     */
    private void initialize(Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex) {
        // get vertex type
        VertexType vt = vertex.getValue().getType();

        // initialize vertex vector, i.e., the theta for doc and phi for word in LDA
        double[] vertexValues = new double[numTopics];
        vertex.getValue().setVector(new DenseVector(vertexValues));

        // initialize edge vector, i.e., the gamma in LDA
        Random rand1 = new Random(vertex.getId().get());
        long seed1 = rand1.nextInt();
        double maxDelta = 0d;
        double sumWeights = 0d;
        for (Edge<LongWritable, DoubleWithVectorWritable> edge : vertex.getMutableEdges()) {
            double weight = edge.getValue().getData();
            if (weight < minVal || weight > maxVal) {
                throw new IllegalArgumentException(String.format("Vertex ID: %d has an edge with weight value " +
                        "out of the range of [%f, %f].", vertex.getId().get(), minVal, maxVal));
            }
            // generate the random seed for this edge
            Random rand2 = new Random(edge.getTargetVertexId().get());
            long seed2 = rand2.nextInt();
            long seed =  seed1 + seed2;
            Random rand = new Random(seed);
            double[] edgeValues = new double[numTopics];
            for (int i = 0; i < numTopics; i++) {
                edgeValues[i] = rand.nextDouble();
            }
            Vector vector = new DenseVector(edgeValues);
            vector = vector.normalize(1d);
            edge.getValue().setVector(vector);
            // find the max delta among all edges
            double delta = vector.norm(1d) / numTopics;
            if (delta > maxDelta) {
                maxDelta = delta;
            }
            // the sum of weights from all edges
            sumWeights += weight;
        }
        // update vertex value
        updateVertex(vertex);
        // aggregate max delta value
        aggregate(MAX_DELTA, new DoubleWritable(maxDelta));
        // aggregate sum of occurrences
        if (vt == VertexType.WORD) {
            aggregate(SUM_OCCURRENCE_COUNT, new DoubleWritable(sumWeights));
        }

        // collect graph statistics
        switch (vt) {
        case DOC:
            aggregate(SUM_DOC_VERTEX_COUNT, new LongWritable(1));
            break;
        case WORD:
            aggregate(SUM_WORD_VERTEX_COUNT, new LongWritable(1));
            break;
        default:
            throw new IllegalArgumentException("Unknow recognized vertex type: " + vt.toString());
        }

        // send out messages
        IdWithVectorWritable newMessage = new IdWithVectorWritable(vertex.getId().get(), vertex.getValue().getVector());
        sendMessageToAllEdges(vertex, newMessage);
    }

    /**
     * update vertex value according to edge value
     *
     * @param vertex of the graph
     */
    private void updateVertex(Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex) {
        VertexType vt = vertex.getValue().getType();
        Vector vector = vertex.getValue().getVector().clone().assign(0d);
        for (Edge<LongWritable, DoubleWithVectorWritable> edge : vertex.getEdges()) {
            double weight = edge.getValue().getData();
            Vector gamma = edge.getValue().getVector();
            vector = vector.plus(gamma.times(weight));
        }
        vertex.getValue().setVector(vector);
        if (vt == VertexType.WORD) {
            aggregate(SUM_WORD_VERTEX_VALUE, new VectorWritable(vector));
        }
    }

    /**
     * update edge value according to vertex and messages
     *
     * @param vertex of the graph
     * @param messages of type iterable
     */
    private void updateEdge(Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex,
        Iterable<IdWithVectorWritable> messages) {
        VertexType vt = vertex.getValue().getType();
        Vector vector = vertex.getValue().getVector();
        // collect messages
        HashMap<Long, Vector> map = new HashMap<Long, Vector>();
        for (IdWithVectorWritable message : messages) {
            map.put(message.getData(), message.getVector());
        }
        if (map.size() != vertex.getNumEdges()) {
            throw new IllegalArgumentException(String.format("Vertex ID %d: Number of received messages isn't equal" +
                "to number of edges.", vertex.getId().get()));
        }
        double maxDelta = 0d;
        for (Edge<LongWritable, DoubleWithVectorWritable> edge : vertex.getMutableEdges()) {
            Vector gamma = edge.getValue().getVector();
            long id = edge.getTargetVertexId().get();
            if (map.containsKey(id)) {
                Vector otherVector = map.get(id);
                Vector newGamma = null;
                switch (vt) {
                case DOC:
                    newGamma = vector.minus(gamma).plus(alpha).times(otherVector.minus(gamma).plus(beta))
                        .times(nk.minus(gamma).plus(numWords * beta).assign(Functions.INV));
                    break;
                case WORD:
                    newGamma = vector.minus(gamma).plus(beta).times(otherVector.minus(gamma).plus(alpha))
                        .times(nk.minus(gamma).plus(numWords * beta).assign(Functions.INV));
                    break;
                default:
                    throw new IllegalArgumentException("Unknow recognized vertex type: " + vt.toString());
                }
                newGamma = newGamma.normalize(1d);
                double delta = gamma.minus(newGamma).norm(1d) / numTopics;
                if (delta > maxDelta) {
                    maxDelta = delta;
                }
                // update edge vector
                edge.getValue().setVector(newGamma);
            } else {
                throw new IllegalArgumentException(String.format("Vertex ID %d: A message is mis-matched",
                    vertex.getId().get()));
            }
        }
        aggregate(MAX_DELTA, new DoubleWritable(maxDelta));
    }

    /**
     * normalize vertex value
     *
     * @param vertex of the graph
     */
    private void normalizeVertex(Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex) {
        VertexType vt = vertex.getValue().getType();
        Vector vector = vertex.getValue().getVector();
        switch (vt) {
        case DOC:
            vector = vector.plus(alpha).normalize(1d);
            break;
        case WORD:
            vector = vector.plus(beta).times(nk.plus(numWords * beta).assign(Functions.INV));
            break;
        default:
            throw new IllegalArgumentException("Unknow recognized vertex type: " + vt.toString());
        }
        // update vertex value
        vertex.getValue().setVector(vector);
    }

    /**
     * evaluate cost according to vertex and messages
     *
     * @param vertex of the graph
     * @param messages of type iterable
     */
    private void evaluateCost(Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex,
        Iterable<IdWithVectorWritable> messages) {
        VertexType vt = vertex.getValue().getType();
        Vector vector = vertex.getValue().getVector();

        if (vt == VertexType.DOC) {
            return;
        }
        vector = vector.plus(beta).times(nk.plus(numWords * beta).assign(Functions.INV));
        // collect messages
        HashMap<Long, Vector> map = new HashMap<Long, Vector>();
        for (IdWithVectorWritable message : messages) {
            map.put(message.getData(), message.getVector());
        }
        if (map.size() != vertex.getNumEdges()) {
            throw new IllegalArgumentException(String.format("Vertex ID %d: Number of received messages isn't equal" +
                "to number of edges.", vertex.getId().get()));
        }
        double cost = 0d;
        for (Edge<LongWritable, DoubleWithVectorWritable> edge : vertex.getEdges()) {
            double weight = edge.getValue().getData();
            long id = edge.getTargetVertexId().get();
            if (map.containsKey(id)) {
                Vector otherVector = map.get(id);
                otherVector = otherVector.plus(alpha).normalize(1d);
                cost -= weight * Math.log(vector.dot(otherVector));
            } else {
                throw new IllegalArgumentException(String.format("Vertex ID %d: A message is mis-matched",
                                vertex.getId().get()));
            }
        }
        aggregate(SUM_COST, new DoubleWritable(cost));
    }

    @Override
    public void compute(Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex,
        Iterable<IdWithVectorWritable> messages) throws IOException {
        long step = getSuperstep();
        if (step == 0) {
            initialize(vertex);
            vertex.voteToHalt();
            return;
        }

        if (step < maxSupersteps) {
            if (costEval) {
                evaluateCost(vertex, messages);
            }
            updateEdge(vertex, messages);
            updateVertex(vertex);

            // send out messages
            IdWithVectorWritable newMessage = new IdWithVectorWritable(vertex.getId().get(),
                vertex.getValue().getVector());
            sendMessageToAllEdges(vertex, newMessage);
        } else {
            // normalize vertex value, i.e., theta and phi in LDA, for final output
            normalizeVertex(vertex);
        }

        vertex.voteToHalt();
    }

    /**
     * Master compute associated with {@link CVB0LDAComputation}. It registers required aggregators.
     */
    public static class CVB0LDAMasterCompute extends DefaultMasterCompute {
        /** whether evaluate cost or not */
        private boolean costEval = false;

        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerPersistentAggregator(SUM_DOC_VERTEX_COUNT, LongSumAggregator.class);
            registerPersistentAggregator(SUM_WORD_VERTEX_COUNT, LongSumAggregator.class);
            registerPersistentAggregator(SUM_OCCURRENCE_COUNT, DoubleSumAggregator.class);
            registerAggregator(SUM_WORD_VERTEX_VALUE, VectorSumAggregator.class);
            registerAggregator(MAX_DELTA, DoubleMaxAggregator.class);
            costEval = getConf().getBoolean(COST_EVAL, false);
            if (costEval) {
                registerAggregator(SUM_COST, DoubleSumAggregator.class);
            }
        }

        @Override
        public void compute() {
            long step = getSuperstep();
            if (step > 0) {
                // store number of edges for graph statistics
                if (step == 1) {
                    long numEdges = getTotalNumEdges() / 2;
                    getConf().setLong(NUM_EDGES, numEdges);
                }
                // evaluate convergence condition
                float threshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                float prevMaxDelta = getConf().getFloat(PREV_MAX_DELTA, 0f);
                if (costEval) {
                    DoubleWritable sumCost = getAggregatedValue(SUM_COST);
                    double cost = 0d;
                    if (step == 1) {
                        double numWords = this.<LongWritable>getAggregatedValue(SUM_WORD_VERTEX_COUNT).get();
                        cost = Math.log(numWords);
                    } else {
                        double numOccurrances = this.<DoubleWritable>getAggregatedValue(SUM_OCCURRENCE_COUNT).get();
                        cost = sumCost.get() / numOccurrances;
                    }
                    sumCost.set(cost);
                }
                double maxDelta = this.<DoubleWritable>getAggregatedValue(MAX_DELTA).get();
                if (Math.abs(prevMaxDelta - maxDelta) < threshold) {
                    getConf().setInt(MAX_SUPERSTEPS, (int) step);
                }
                getConf().setFloat(PREV_MAX_DELTA, (float) maxDelta);
            }
        }
    }

    /**
     * This is an aggregator writer for LDA, which after each super step will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class CVB0LDAAggregatorWriter extends DefaultImmutableClassesGiraphConfigurable
        implements AggregatorWriter {
        /** Name of the file we wrote to */
        private static String FILENAME;
        /** Saved output stream to write to */
        private FSDataOutputStream output;

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
         * @param applicationAttempt
         *            app attempt
         */
        private static void setFilename(long applicationAttempt) {
            FILENAME = "lda-learning-report_" + applicationAttempt;
        }

        @Override
        public void writeAggregator(Iterable<Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {
            // collect aggregator data
            HashMap<String, String> map = new HashMap<String, String>();
            for (Entry<String, Writable> entry : aggregatorMap) {
                map.put(entry.getKey(), entry.getValue().toString());
            }

            boolean costEval = getConf().getBoolean(COST_EVAL, false);
            int maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 20);
            int realStep = 0;
            if (superstep >= 1) {
                realStep = (int) superstep - 1;
            } else if (superstep == -1) {
                realStep = maxSupersteps;
            }
            if (superstep == 0) {
                // output graph statistics
                long numDocVertices = Long.parseLong(map.get(SUM_DOC_VERTEX_COUNT));
                long numWordVertices = Long.parseLong(map.get(SUM_WORD_VERTEX_COUNT));
                long numEdges = getConf().getLong(NUM_EDGES, 0L);
                output.writeUTF("Graph Statistics:\n");
                output.writeUTF(String.format("Number of vertices: %d (doc: %d, word: %d)%n",
                    numDocVertices + numWordVertices, numDocVertices, numWordVertices));
                output.writeUTF(String.format("Number of edges: %d%n", numEdges));
                output.writeUTF("\n");
                // output LDA configuration
                int numTopics = getConf().getInt(NUM_TOPICS, 10);
                float alpha = getConf().getFloat(ALPHA, 0.1f);
                float beta = getConf().getFloat(BETA, 0.1f);
                float convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                float maxVal = getConf().getFloat(MAX_VAL, Float.POSITIVE_INFINITY);
                float minVal = getConf().getFloat(MIN_VAL, Float.NEGATIVE_INFINITY);
                output.writeBytes("======================LDA Configuration====================\n");
                output.writeBytes("numTopics: " + numTopics + "\n");
                output.writeBytes("alpha: " + alpha + "\n");
                output.writeBytes("beta: " + beta + "\n");
                output.writeBytes("convergenceThreshold: n" + convergenceThreshold + "\n");
                output.writeBytes("maxSupersteps: " +  maxSupersteps + "\n");
                output.writeBytes("maxVal: " +  maxVal + "\n");
                output.writeBytes("minVal: " + minVal + "\n");
                output.writeBytes("evaluateCost: " + costEval + "\n");
                output.writeBytes("-------------------------------------------------------------\n");
                output.writeBytes("\n");
                output.writeBytes("========================Learning Progress====================\n");
            }
            if (realStep > 0) {
                // output learning progress
                output.writeBytes("superstep = " + realStep + "\t");
                if (costEval) {
                    double cost = Double.parseDouble(map.get(SUM_COST));
                    output.writeBytes("cost = " + cost + "\t");
                }
                double maxDelta = Double.parseDouble(map.get(MAX_DELTA));
                output.writeBytes("maxDelta = " + maxDelta + "\n");
            }
            output.flush();
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

}
