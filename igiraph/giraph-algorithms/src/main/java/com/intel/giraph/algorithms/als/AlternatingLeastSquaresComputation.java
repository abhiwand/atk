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

package com.intel.giraph.algorithms.als;

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
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DiagonalMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.QRDecomposition;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;

import com.intel.giraph.io.EdgeDataWritable;
import com.intel.giraph.io.MessageDataWritable;
import com.intel.giraph.io.EdgeDataWritable.EdgeType;
import com.intel.giraph.io.VertexDataWritable;
import com.intel.giraph.io.VertexDataWritable.VertexType;

/**
 * Alternating Least Squares with Bias for collaborative filtering
 * The algorithms presented in
 * (1) Y. Zhou, D. Wilkinson, R. Schreiber and R. Pan. Large-Scale
 * Parallel Collaborative Filtering for the Netflix Prize. 2008.
 * (2) Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative
 * Filtering Model. In ACM KDD 2008. (Equation 5)
 */
@Algorithm(
    name = "Alternating Least Squares with Bias"
)
public class AlternatingLeastSquaresComputation extends BasicComputation<LongWritable, VertexDataWritable,
    EdgeDataWritable, MessageDataWritable> {
    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "als.maxSupersteps";
    /** Custom argument for feature dimension */
    public static final String FEATURE_DIMENSION = "als.featureDimension";
    /**
     * Custom argument for regularization parameter: lambda
     * f = L2_error + lambda*Tikhonov_regularization
     */
    public static final String LAMBDA = "als.lambda";
    /** Custom argument to turn on/off bias */
    public static final String BIAS_ON = "als.biasOn";
    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "als.convergenceThreshold";
    /** Custom argument for maximum edge weight value */
    public static final String MAX_VAL = "als.maxVal";
    /** Custom argument for minimum edge weight value */
    public static final String MIN_VAL = "als.minVal";
    /**
     * Custom argument for learning curve output interval (default: every iteration)
     * Since each ALS iteration is composed by 2 super steps, one iteration
     * means two super steps.
     * */
    public static final String LEARNING_CURVE_OUTPUT_INTERVAL = "als.learningCurveOutputInterval";

    /** Aggregator name for sum of cost on training data */
    private static String SUM_TRAIN_COST = "cost_train";
    /** Aggregator name for sum of L2 error on validate data */
    private static String SUM_VALIDATE_ERROR = "validate_rmse";
    /** Aggregator name for sum of L2 error on test data */
    private static String SUM_TEST_ERROR = "test_rmse";
    /** Aggregator name for number of left vertices */
    private static String SUM_LEFT_VERTICES = "num_left_vertices";
    /** Aggregator name for number of right vertices */
    private static String SUM_RIGHT_VERTICES = "num_right_vertices";
    /** Aggregator name for number of training edges */
    private static String SUM_TRAIN_EDGES = "num_train_edges";
    /** Aggregator name for number of validate edges */
    private static String SUM_VALIDATE_EDGES = "num_validate_edges";
    /** Aggregator name for number of test edges */
    private static String SUM_TEST_EDGES = "num_test_edges";
    /** RMSE value of previous iteration for convergence monitoring */
    private static String PREV_RMSE = "prev_rmse";

    /** Number of super steps */
    private int maxSupersteps = 20;
    /** Feature dimension */
    private int featureDimension = 20;
    /** The regularization parameter */
    private float lambda = 0f;
    /** Bias on/off switch */
    private boolean biasOn = false;
    /** Maximum edge weight value */
    private float maxVal = Float.POSITIVE_INFINITY;
    /** Minimum edge weight value */
    private float minVal = Float.NEGATIVE_INFINITY;
    /** Iteration interval to output learning curve */
    private int learningCurveOutputInterval = 1;

    @Override
    public void preSuperstep() {
        // Set custom parameters
        maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 20);
        featureDimension = getConf().getInt(FEATURE_DIMENSION, 20);
        if (featureDimension < 1) {
            throw new IllegalArgumentException("Feature dimension should be > 0.");
        }
        lambda = getConf().getFloat(LAMBDA, 0f);
        if (lambda < 0) {
            throw new IllegalArgumentException("Regularization parameter lambda should be >= 0.");
        }
        biasOn = getConf().getBoolean(BIAS_ON, false);
        maxVal = getConf().getFloat(MAX_VAL, Float.POSITIVE_INFINITY);
        minVal = getConf().getFloat(MIN_VAL, Float.NEGATIVE_INFINITY);
        learningCurveOutputInterval = getConf().getInt(LEARNING_CURVE_OUTPUT_INTERVAL, 1);
        if (learningCurveOutputInterval < 1) {
            throw new IllegalArgumentException("Learning curve output interval should be >= 1.");
        }
    }

    /**
     * initialize vertex, collect graph statistics and send out messages
     *
     * @param vertex of the graph
     */
    private void initialize(Vertex<LongWritable, VertexDataWritable, EdgeDataWritable> vertex) {
        // initialize vertex data: bias, vector, gradient, conjugate
        vertex.getValue().setBias(0d);

        double sum = 0d;
        int numTrain = 0;
        for (Edge<LongWritable, EdgeDataWritable> edge : vertex.getEdges()) {
            EdgeType et = edge.getValue().getType();
            if (et == EdgeType.TRAIN) {
                double weight = edge.getValue().getWeight();
                if (weight < minVal || weight > maxVal) {
                    throw new IllegalArgumentException(String.format("Vertex ID: %d has an edge with weight value " +
                        "out of the range of [%f, %f].", vertex.getId().get(), minVal, maxVal));
                }
                sum += weight;
                numTrain++;
            }
        }
        Random rand = new Random(vertex.getId().get());
        double[] values = new double[featureDimension];
        values[0] = 0d;
        if (numTrain > 0) {
            values[0] = sum / numTrain;
        }
        for (int i = 1; i < featureDimension; i++) {
            double sample = rand.nextDouble() * values[0];
            values[i] = sample;
        }
        vertex.getValue().setVector(new DenseVector(values));

        // collect graph statistics and send out messages
        VertexType vt = vertex.getValue().getType();
        switch (vt) {
        case LEFT:
            aggregate(SUM_LEFT_VERTICES, new LongWritable(1));
            break;
        case RIGHT:
            aggregate(SUM_RIGHT_VERTICES, new LongWritable(1));
            long numTrainEdges = 0L;
            long numValidateEdges = 0L;
            long numTestEdges = 0L;
            for (Edge<LongWritable, EdgeDataWritable> edge : vertex.getEdges()) {
                EdgeType et = edge.getValue().getType();
                switch (et) {
                case TRAIN:
                    numTrainEdges++;
                    break;
                case VALIDATE:
                    numValidateEdges++;
                    break;
                case TEST:
                    numTestEdges++;
                    break;
                default:
                    throw new IllegalArgumentException("Unknow recognized edge type: " + et.toString());
                }
                // send out messages
                MessageDataWritable newMessage = new MessageDataWritable(vertex.getValue(), edge.getValue());
                sendMessage(edge.getTargetVertexId(), newMessage);
            }
            if (numTrainEdges > 0) {
                aggregate(SUM_TRAIN_EDGES, new LongWritable(numTrainEdges));
            }
            if (numValidateEdges > 0) {
                aggregate(SUM_VALIDATE_EDGES, new LongWritable(numValidateEdges));
            }
            if (numTestEdges > 0) {
                aggregate(SUM_TEST_EDGES, new LongWritable(numTestEdges));
            }
            break;
        default:
            throw new IllegalArgumentException("Unknow recognized vertex type: " + vt.toString());
        }
    }

    /**
     * compute bias
     *
     * @param value of type Vector
     * @param messages of type Iterable
     * @return bias of type double
     */
    private double computeBias(Vector value, Iterable<MessageDataWritable> messages) {
        double errorOnTrain = 0d;
        int numTrain = 0;
        for (MessageDataWritable message : messages) {
            EdgeType et = message.getType();
            if (et == EdgeType.TRAIN) {
                double weight = message.getWeight();
                Vector vector = message.getVector();
                double otherBias = message.getBias();
                double predict = otherBias + value.dot(vector);
                double e = weight - predict;
                errorOnTrain += e;
                numTrain++;
            }
        }
        double bias = 0d;
        if (numTrain > 0) {
            bias = errorOnTrain / ((1 + lambda) * numTrain);
        }
        return bias;
    }

    @Override
    public void compute(Vertex<LongWritable, VertexDataWritable, EdgeDataWritable> vertex,
        Iterable<MessageDataWritable> messages) throws IOException {
        long step = getSuperstep();
        if (step == 0) {
            initialize(vertex);
            vertex.voteToHalt();
            return;
        }

        Vector currentValue = vertex.getValue().getVector();
        double currentBias = vertex.getValue().getBias();
        // update aggregators every (2 * interval) super steps
        if ((step % (2 * learningCurveOutputInterval)) == 0) {
            double errorOnTrain = 0d;
            double errorOnValidate = 0d;
            double errorOnTest = 0d;
            int numTrain = 0;
            for (MessageDataWritable message : messages) {
                EdgeType et = message.getType();
                double weight = message.getWeight();
                Vector vector = message.getVector();
                double otherBias = message.getBias();
                double predict = currentBias + otherBias + currentValue.dot(vector);
                double e = weight - predict;
                switch (et) {
                case TRAIN:
                    errorOnTrain += e * e;
                    numTrain++;
                    break;
                case VALIDATE:
                    errorOnValidate += e * e;
                    break;
                case TEST:
                    errorOnTest += e * e;
                    break;
                default:
                    throw new IllegalArgumentException("Unknow recognized edge type: " + et.toString());
                }
            }
            double costOnTrain = 0d;
            if (numTrain > 0) {
                costOnTrain = errorOnTrain / numTrain + lambda * (currentBias * currentBias +
                    currentValue.dot(currentValue));
            }
            aggregate(SUM_TRAIN_COST, new DoubleWritable(costOnTrain));
            aggregate(SUM_VALIDATE_ERROR, new DoubleWritable(errorOnValidate));
            aggregate(SUM_TEST_ERROR, new DoubleWritable(errorOnTest));
        }

        // update vertex value
        if (step < maxSupersteps) {
            // xxt records the result of x times x transpose
            Matrix xxt = new DenseMatrix(featureDimension, featureDimension);
            xxt = xxt.assign(0d);
            // xr records the result of x times rating
            Vector xr = currentValue.clone().assign(0d);
            int numTrain = 0;
            for (MessageDataWritable message : messages) {
                EdgeType et = message.getType();
                if (et == EdgeType.TRAIN) {
                    double weight = message.getWeight();
                    Vector vector = message.getVector();
                    double otherBias = message.getBias();
                    xxt = xxt.plus(vector.cross(vector));
                    xr = xr.plus(vector.times(weight - currentBias - otherBias));
                    numTrain++;
                }
            }
            xxt = xxt.plus(new DiagonalMatrix(lambda * numTrain, featureDimension));
            Matrix bMatrix = new DenseMatrix(featureDimension, 1).assignColumn(0, xr);
            Vector value = new QRDecomposition(xxt).solve(bMatrix).viewColumn(0);
            vertex.getValue().setVector(value);

            // update vertex bias
            if (biasOn) {
                double bias = computeBias(value, messages);
                vertex.getValue().setBias(bias);
            }

            // send out messages
            for (Edge<LongWritable, EdgeDataWritable> edge : vertex.getEdges()) {
                MessageDataWritable newMessage = new MessageDataWritable(vertex.getValue(), edge.getValue());
                sendMessage(edge.getTargetVertexId(), newMessage);
            }
        }

        vertex.voteToHalt();
    }

    /**
     * Master compute associated with {@link SimplePageRankComputation}. It registers required aggregators.
     */
    public static class AlternatingLeastSquaresMasterCompute extends DefaultMasterCompute {
        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerAggregator(SUM_TRAIN_COST, DoubleSumAggregator.class);
            registerAggregator(SUM_VALIDATE_ERROR, DoubleSumAggregator.class);
            registerAggregator(SUM_TEST_ERROR, DoubleSumAggregator.class);
            registerPersistentAggregator(SUM_LEFT_VERTICES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_RIGHT_VERTICES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_TRAIN_EDGES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_VALIDATE_EDGES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_TEST_EDGES, LongSumAggregator.class);
        }

        @Override
        public void compute() {
            // check convergence at every (2 * interval) super steps
            int learningCurveOutputInterval = getConf().getInt(LEARNING_CURVE_OUTPUT_INTERVAL, 1);
            long step = getSuperstep();
            if (step < 3 || (step % (2 * learningCurveOutputInterval)) == 0) {
                return;
            }
            // calculate rmse on validate data
            DoubleWritable sumValidateError = getAggregatedValue(SUM_VALIDATE_ERROR);
            LongWritable numValidateEdges = getAggregatedValue(SUM_VALIDATE_EDGES);
            double validateRmse = 0d;
            if (numValidateEdges.get() > 0) {
                validateRmse = sumValidateError.get() / numValidateEdges.get();
                validateRmse = Math.sqrt(validateRmse);
            }
            sumValidateError.set(validateRmse);
            // calculate rmse on test data
            DoubleWritable sumTestError = getAggregatedValue(SUM_TEST_ERROR);
            LongWritable numTestEdges = getAggregatedValue(SUM_TEST_EDGES);
            double testRmse = 0d;
            if (numTestEdges.get() > 0) {
                testRmse = sumTestError.get() / numTestEdges.get();
                testRmse = Math.sqrt(testRmse);
            }
            sumTestError.set(testRmse);
            // evaluate convergence condition
            if (step >= 3 + (2 * learningCurveOutputInterval)) {
                float prevRmse = getConf().getFloat(PREV_RMSE, 0f);
                float threshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                if (Math.abs(prevRmse - validateRmse) < threshold) {
                    haltComputation();
                }
            }
            getConf().setFloat(PREV_RMSE, (float) validateRmse);
        }
    }

    /**
     * This is an aggregator writer for ALS, which after each superstep will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class AlternatingLeastSquaresAggregatorWriter extends DefaultImmutableClassesGiraphConfigurable
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
            FILENAME = "als-learning-report_" + applicationAttempt;
        }

        @Override
        public void writeAggregator(Iterable<Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {
            // collect aggregator data
            HashMap<String, String> map = new HashMap<String, String>();
            for (Entry<String, Writable> entry : aggregatorMap) {
                map.put(entry.getKey(), entry.getValue().toString());
            }

            int learningCurveOutputInterval = getConf().getInt(LEARNING_CURVE_OUTPUT_INTERVAL, 1);
            if (superstep == 1) {
                // output graph statistics
                long leftVertices = Long.parseLong(map.get(SUM_LEFT_VERTICES));
                long rightVertices = Long.parseLong(map.get(SUM_RIGHT_VERTICES));
                long trainEdges = Long.parseLong(map.get(SUM_TRAIN_EDGES));
                long validateEdges = Long.parseLong(map.get(SUM_VALIDATE_EDGES));
                long testEdges = Long.parseLong(map.get(SUM_TEST_EDGES));
                output.writeUTF("Graph Statistics:\n");
                output.writeUTF(String.format("Number of vertices: %d (left: %d, right: %d)%n",
                    leftVertices + rightVertices, leftVertices, rightVertices));
                output.writeUTF(String.format("Number of edges: %d (train: %d, validate: %d, test: %d)%n",
                    trainEdges + validateEdges + testEdges, trainEdges, validateEdges, testEdges));
                output.writeUTF("\n");
                // output ALS configuration
                int featureDimension = getConf().getInt(FEATURE_DIMENSION, 20);
                float lambda = getConf().getFloat(LAMBDA, 0f);
                boolean biasOn = getConf().getBoolean(BIAS_ON, false);
                float convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                int maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 20);
                float maxVal = getConf().getFloat(MAX_VAL, Float.POSITIVE_INFINITY);
                float minVal = getConf().getFloat(MIN_VAL, Float.NEGATIVE_INFINITY);
                output.writeUTF("ALS Configuration:\n");
                output.writeUTF(String.format("featureDimension: %d%n", featureDimension));
                output.writeUTF(String.format("lambda: %f%n", lambda));
                output.writeUTF(String.format("biasOn: %b%n", biasOn));
                output.writeUTF(String.format("convergenceThreshold: %f%n", convergenceThreshold));
                output.writeUTF(String.format("maxSupersteps: %d%n", maxSupersteps));
                output.writeUTF(String.format("maxVal: %f%n", maxVal));
                output.writeUTF(String.format("minVal: %f%n", minVal));
                output.writeUTF(String.format("learningCurveOutputInterval: %d%n", learningCurveOutputInterval));
                output.writeUTF("\n");
                output.writeUTF("Learning Progress:\n");
            } else if ((superstep >= 3 && (superstep % (2 * learningCurveOutputInterval)) == 1) || superstep == -1) {
                // output learning progress
                double trainCost = Double.parseDouble(map.get(SUM_TRAIN_COST));
                double validateRmse = Double.parseDouble(map.get(SUM_VALIDATE_ERROR));
                double testRmse = Double.parseDouble(map.get(SUM_TEST_ERROR));
                output.writeUTF(String.format("superstep=%d", superstep));
                output.writeUTF(String.format("cost(train)=%f", trainCost));
                output.writeUTF(String.format("rmse(validate)=%f", validateRmse));
                output.writeUTF(String.format("rmse(test)=%f", testRmse));
                output.writeUTF("\n");
            }
            output.flush();
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

}
