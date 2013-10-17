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

package com.intel.giraph.algorithms.kmeanspp;

import java.io.IOException;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.aggregators.LongSumAggregator;

import com.intel.giraph.aggregators.VectorWritableOverwriteAggregator;
import com.intel.giraph.aggregators.LongWritableOverwriteAggregator;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.DenseVector;

import org.apache.log4j.Logger;

import com.intel.mahout.math.IdWithVectorWritable;

/**
 * KMeans++ algorithm.
 */
@Algorithm(
    name = "KMeans++"
)
public class KMeansppComputation extends 
    BasicComputation<LongWritable, IdWithVectorWritable, NullWritable, IdWithVectorWritable> {

    /** Custom argument for the max number of super steps */
    public static final String MAX_SUPERSTEPS = "kmeanspp.maxSupersteps";
    /** Custom argument for the number of clusters */
    public static final String NUM_CENTEROIDS = "kmeanspp.numCenteroids";
    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "kmeanspp.convergenceThreshold";

    /** Number of super steps */
    private int maxSupersteps = 10;
    /** Number of centeroids */
    private static int numCenteroids;
    /** Number of datapoints */
    private static long numDatapoints = 0;
    /** The convergence threshold controlling if sending message */
    private float convergenceThreshold = 0.001f;

    /** KMeans++ - Phases in finding initial centeroids */
    private static final long PHASE_DISTRIBUTE_POSITION = 0;
    private static final long PHASE_COMPUTE_DISTANCE = 1;
    private static final long PHASE_FINDING_COMPLETE = 2;
    private static final long PHASE_RUN_KMEANS = 3;

    /** Aggregator to exchange values between the MasterCompute and workers. */
    public static final String KMEANSPP_INIT_CENTEROID_ID = "kmeanspp.agg.init_centeroid_id";
    public static final String KMEANSPP_CENTEROID_COUNT = "kmeanspp.agg.centeroid_count";
    public static final String KMEANSPP_CENTEROID = "centeroid";
    public static final String KMEANSPP_SUM_DISTANCE = "kmeanspp.agg.sum_distance";
    public static final String KMEANSPP_PHASE = "kmeanspp.agg.phase";
    public static final String KMEANSPP_NOT_CONVERGED = "kmeanspp.agg.not_converged";

    /** Random number generator.
    * TODO: we might need fixed seed for QA purpose. */
    Random rand_gen = new Random();

    /** Class logger */
    private static Logger LOG = Logger.getLogger(KMeansppComputation.class);

    @Override
    public void preSuperstep() {
        maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
        convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
        numCenteroids = getConf().getInt(NUM_CENTEROIDS, 2);

        if (getSuperstep() == 0) {
            numDatapoints = getTotalNumVertices();
        }
    }

    /**
     * Adding a centeroid node that computes the distances and selects the next centeroid.
     *
     * @param new_vid Centeroid node's vertex id.
     * @param centeroid KMeans++'s randomly selected initial centeroid.
     */
    private void addCenteroid(long new_vid, Vector centeroid) throws IOException {
        LOG.info("KMeans++: adding centeroid node to compute centeroid with the vid=" + new_vid);
        LongWritable centeroid_vid = new LongWritable(new_vid);
        addVertexRequest(centeroid_vid, new IdWithVectorWritable(-1, centeroid));

        LOG.info("KMeans++: adding edges between the centeroid and datapoints.");
        for (long datapoint_id = 0; datapoint_id < numDatapoints; datapoint_id++) {
            LongWritable vid = new LongWritable(datapoint_id);
            addEdgeRequest(vid, EdgeFactory.create(centeroid_vid, NullWritable.get()));
            addEdgeRequest(centeroid_vid, EdgeFactory.create(vid, NullWritable.get()));
        }
    }

    /**
     * Computes the "squred" eucledian distance between two vectors for KMeans++.
     *
     * @param v1 first vector.
     * @param v2 second vector.
     *
     * @return Squared eucledian distance.
     */
    private double computeSquaredEucledianDistance(Vector v1, Vector v2) {
        double distance = 0.0;
        for (int i = 0; i < v1.size(); i++) {
            distance += Math.pow(v1.get(i) - v2.get(i), 2);
        }
        return distance;
    }

    /**
     * Computes the shortest distance between a datapoint and centeroids.
     *
     * @param vec_list List of centeroid vectors.
     * @param vec2 Datapoint vector.
     *
     * @return shortest distance.
     */
    private double computeShortestDistance(List<Vector> vec_list, Vector vec2) {
        double current_distance = Double.POSITIVE_INFINITY;
        for (Vector vec1 : vec_list) {
            double distance = computeSquaredEucledianDistance(vec1, vec2);
            LOG.debug("KMeans++: vec1=" + vec1 + ", vec2=" + vec2 + ", dist=" + distance);
            if (distance < current_distance) {
                current_distance = distance;
            }
        }
        LOG.debug("KMeans++: shortest distnce = " + current_distance);
        return current_distance;
    }

    /**
     * Check if the current vertex is master centeroid.
     *
     * @param my_vid Vertex ID.
     *
     * @return true if the vertex is master centeroid. false otherwise.
     */
    private boolean isCenteroid(long my_vid) {
        return !(my_vid < numDatapoints);
    }

    /**
     * Check if the current vertex is datapoint.
     *
     * @param my_vid Vertex ID.
     *
     * @return true if the vertex is datapoint. false otherwise.
     */
    private boolean isDatapoint(long my_vid) {
        return (my_vid < numDatapoints);
    }

    /**
     * Broadcast centeroid vector to datapoints.
     *
     */
    private void broadcastCenteroid(long vid, Vector vec, Iterable<Edge<LongWritable, NullWritable>> edges) {
        //for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
        for (Edge<LongWritable, NullWritable> edge : edges) {
            LOG.debug("KMeans++: the centeroid " + vid + " sends its vector to datapoints.");
            //sendMessage(edge.getTargetVertexId(), new IdWithVectorWritable(vid, vertex.getValue().get()));
            sendMessage(edge.getTargetVertexId(), new IdWithVectorWritable(vid, vec));
        }
    }


    /**
     * Returns the sum of two vectors.
     *
     * @param v1 first vector.
     * @param v2 second vector.
     *
     * @return Resulting vector.
     */
    private Vector addTwoVector(Vector v1, Vector v2) {
        int len = v1.size();
        double[] vec = new double[len];
        for (int i = 0; i < len; i++) {
            vec[i] = v1.get(i) + v2.get(i);
        }
        return new DenseVector(vec);
    }

    /**
     * Computes the new centeroid vector based on received datapoints.
     *
     * @return The new centeroid vector.
     */
    private Vector computeCenteroid(Iterable<IdWithVectorWritable> messages) {
        boolean init = false;
        Vector result_vec = new DenseVector();
        long num_messages = 0;
        for (IdWithVectorWritable message : messages) {
            num_messages++;
            Vector v = message.getVector();
            LOG.debug("KMeans++: computeCenteroid v = " + v);
            if (!init) {
               result_vec = v.clone(); 
                LOG.debug("KMeans++: computeCenteroid result_vec + v = " + result_vec);
               init = true;
               continue;
            }
            result_vec = addTwoVector(result_vec, v);
            LOG.debug("KMeans++: computeCenteroid result_vec + v = " + result_vec);
        }

        int dimension = result_vec.size();
        for (int i = 0; i < dimension; i++) {
            result_vec.set(i, result_vec.get(i) / (double)num_messages);
        }
        return result_vec;
    }

    /**
     * Returns the array of centeroid vectors.
     *
     * @return Centeroid vectors in array.
     */
    private Vector[] getCenteroidVectorArray() {
        Vector[] centeroid_vec = new Vector[numCenteroids];
        for (int i = 0; i < numCenteroids; i++) {
            centeroid_vec[i] = (((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + i)).get());
        }
        return centeroid_vec;
    }

    /**
     * Returns the centeroid vector of centeroid_id.
     *
     * @param centeroid_id Centeroid ID.
     *
     * @return Conteroid vector.
     */
    private Vector getCenteroidVector(long centeroid_id) {
        return ((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + centeroid_id)).get();
    }

    @Override
    public void compute(Vertex<LongWritable, IdWithVectorWritable, NullWritable> vertex, 
        Iterable<IdWithVectorWritable> messages) throws IOException {

        long step = getSuperstep(); 
        long vid = vertex.getId().get();
        long phase = ((LongWritable) getAggregatedValue(KMEANSPP_PHASE)).get();

        /** number of centeroid that has been selected by kmeans++ */
        long centeroid_count = ((LongWritable) getAggregatedValue(KMEANSPP_CENTEROID_COUNT)).get();

        /** initial random centeroid */
        if (step == 0) {
            /* center seletected -> selected center create centeroid. */
            long init_centeroid = ((LongWritable) 
            getAggregatedValue(KMEANSPP_INIT_CENTEROID_ID)).get() % numDatapoints;

            if (isDatapoint(vid)) {
                if (vid == init_centeroid) {
                    LOG.info("KMeans++: initial centeroid = " + init_centeroid);
                    LOG.info("KMeans++: initial centeroid at " + vertex.getValue().getVector());
                    addCenteroid(numDatapoints + centeroid_count, vertex.getValue().getVector());
                    aggregate(KMEANSPP_CENTEROID + centeroid_count, 
                              new VectorWritable(new DenseVector(vertex.getValue().getVector())));
                }
            }
            vertex.getValue().setData(Long.MAX_VALUE);
            return;
        }

        /** datapoints distribute its position vector to centeroid */
        if (phase == PHASE_DISTRIBUTE_POSITION) {
            if (isDatapoint(vid)) {
                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    LOG.debug("KMeans++: datapoint " + vid + " sends its location to centeroid");
                    sendMessage(edge.getTargetVertexId(), 
                                new IdWithVectorWritable(vid, vertex.getValue().getVector()));
                }
            }
            return;
        }

        /** centeroid computes distance */
        if (phase == PHASE_COMPUTE_DISTANCE) {
            if (isDatapoint(vid)) {
                /** Datapoints has nothing to do in this phase. */
                return;
            }

            LOG.info("KMeans++: centeroid" + vid + " computes the shortest distance.");
            List<Vector> centeroid_vec = new ArrayList<Vector>();
            for (long centeroid_id = 0; centeroid_id < centeroid_count; centeroid_id++) {
                Vector centeroid = ((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + centeroid_id)).get();
                centeroid_vec.add(centeroid);
            }

            /** finding the closest centeroid from this datapont and calculating the distance. */
            List<Double> distance_list = new ArrayList<Double>();
            double sum_distance = 0.0;
            for (IdWithVectorWritable message : messages) {
                double distance = computeShortestDistance(centeroid_vec, message.getVector());
                sum_distance += distance;
                distance_list.add(sum_distance);
            }
          
            int idx = 0;
            double prob = sum_distance * rand_gen.nextDouble();
            for (IdWithVectorWritable message : messages) {
                if (prob < distance_list.get(idx)) {
                    LOG.info("KMeans++: found centeroid" + centeroid_count + " at " + message.getVector());
                    aggregate(KMEANSPP_CENTEROID + centeroid_count, new VectorWritable(message.getVector()));
                    vertex.setValue(new IdWithVectorWritable(-1, message.getVector()));
                    break;
                }
                idx++;
            }
            return;
        } 

        /** KMeans++ initial centeroids finding is done - now performing preparation for running KMeans algorithm */
        if (phase == PHASE_FINDING_COMPLETE) {
            if (isDatapoint(vid)) {
                /** put datapoints into sleep and wake them when all the centeroids are getting active
                 *  in the next phase (PHASE_RUN_KMEANS).
                 */
                vertex.voteToHalt();
                return;
            }

            /** master centeroid preparing for the KMeans by creating new centeroids found in the previous steps. */
            for (long centeroid_id = numDatapoints; centeroid_id < (numDatapoints + numCenteroids); centeroid_id++) {
                Vector centeroid_vec = getCenteroidVector(centeroid_id - numDatapoints);
                LOG.info("KMeans++: new centeroid " + centeroid_id + " with vector " + centeroid_vec);
                if (centeroid_id == numDatapoints) {
                    /** master centeroid is setting itself with a centeroid vector */ 
                    vertex.setValue(new IdWithVectorWritable(-1, centeroid_vec));
                } else {
                    /** master centeroid creates the reset of (k-1) centeroids and set them with centeroid vectors */
                    addCenteroid(centeroid_id, centeroid_vec);
                }
            }
            return;
        }

        if (phase == PHASE_RUN_KMEANS) {
            if (isCenteroid(vid)) {
                LOG.info("KMeans++: the centeroid " + vid + " calculates the new centeroid.");
                Vector new_centeroid_vec = computeCenteroid(messages);

                if (new_centeroid_vec.size() > 0) {
                    /** the new centeroid has been calculated - broadcast the new centeroid! */
                    LOG.info ("KMeans++: broadcast the new centeroid " + new_centeroid_vec);

                    vertex.setValue(new IdWithVectorWritable(-1, new_centeroid_vec));

                    broadcastCenteroid(vid, new_centeroid_vec, vertex.getEdges());
                } else {
                    /** centeroid starts KMeans with broadcasting its location to datapoints */
                    LOG.info ("KMeans++: broadcast the initial centeroid " + vertex.getValue().getVector());
                    broadcastCenteroid(vid, vertex.getValue().getVector(), vertex.getEdges());
                }

                /** set new centeroid */
                long centeroid_num = vid - numDatapoints;
                LOG.info("KMeans++: aggregate to " + KMEANSPP_CENTEROID + centeroid_num);
                aggregate(KMEANSPP_CENTEROID + centeroid_num, new VectorWritable(new_centeroid_vec));

                /** convergence cannot happen yet */
                aggregate(KMEANSPP_NOT_CONVERGED, new LongWritable(1));

                vertex.voteToHalt();
                return;
            }

            if (isDatapoint(vid)) {
                Vector[] vector_array = new Vector[numCenteroids];
                long new_centeroid = Long.MAX_VALUE;
                double shortest_distance = Double.POSITIVE_INFINITY;

                for (IdWithVectorWritable message : messages) {
                    double distance = computeSquaredEucledianDistance(vertex.getValue().getVector(),
                                                                      message.getVector());
                    if (distance < shortest_distance) {
                        shortest_distance = distance;
                        new_centeroid = message.getData();
                    }
                }

                long closest_centeroid = vertex.getValue().getData();
                LOG.debug("KMeans++: old = " + closest_centeroid + ", new = " + new_centeroid);
                if (closest_centeroid != new_centeroid) {
                    closest_centeroid = new_centeroid;
                    vertex.getValue().setData(closest_centeroid);
                    aggregate(KMEANSPP_NOT_CONVERGED, new LongWritable(1));
                }
                sendMessage(new LongWritable(closest_centeroid), 
                            new IdWithVectorWritable(vid, vertex.getValue().getVector()));
                vertex.voteToHalt();
                return;
            }
        }
    }


    /**
     * MasterCompute used with {@link KMeansppComputation}.
     */
    public static class KMeansppMasterCompute extends DefaultMasterCompute {
        /** 
        * KMeans++ - phase transition for finding initial k centeroids.
        */
        private long getNextPhase(long current_phase, long centeroid_count) {
            if (centeroid_count < numCenteroids) {
                if (current_phase == PHASE_DISTRIBUTE_POSITION) {
                    return PHASE_COMPUTE_DISTANCE;
                } else {
                    return PHASE_DISTRIBUTE_POSITION;
                }
            }
            return PHASE_FINDING_COMPLETE;
        }

        private void updateCenteroidAggregator() {
            for (int i = 0; i < numCenteroids; i++) {
                Vector centeroid = ((VectorWritable)getAggregatedValue(KMEANSPP_CENTEROID + i)).get();
                setAggregatedValue(KMEANSPP_CENTEROID + i, new VectorWritable(centeroid));
            }
        }

        @Override
        public void compute() {
            long step = getSuperstep(); 
            
            if (step == 0) {
                /** selects initial centroid uniformly at random */
                Random rand = new Random();
                long init_centeroid = Math.abs(rand.nextLong());
                LOG.info("KMeans++: random centeroid vid = " + init_centeroid);

                /** initialize aggregators with initial values */
                setAggregatedValue(KMEANSPP_INIT_CENTEROID_ID, new LongWritable(init_centeroid));
                setAggregatedValue(KMEANSPP_CENTEROID_COUNT, new LongWritable(0));
                setAggregatedValue(KMEANSPP_PHASE, new LongWritable(-1));
                setAggregatedValue(KMEANSPP_NOT_CONVERGED , new LongWritable(0));
                return;
            }

            /** KMeans++ phase */
            long current_phase = ((LongWritable) getAggregatedValue(KMEANSPP_PHASE)).get(); 
            
            /** KMeans++ finding initial centeroids */
            if (current_phase < PHASE_FINDING_COMPLETE) {
                /** update the aggregators with current centeroids */
                long centeroid_count;
                for (centeroid_count = 0; centeroid_count < numCenteroids; centeroid_count++) {
                    Vector centeroid = ((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + centeroid_count)).get();
                    if (centeroid.size() > 0) {
                        LOG.info("KMeans++: centeroid " + centeroid_count + " at " + centeroid);
                        setAggregatedValue(KMEANSPP_CENTEROID + centeroid_count, new VectorWritable(centeroid));
                        continue;
                    }
                    break;
                }

                /** update the number of found centeroids */
                setAggregatedValue(KMEANSPP_CENTEROID_COUNT, new LongWritable(centeroid_count));

                /** update the next centeroid-finding-phase based on the number of already found centeroids. */
                long next_phase = getNextPhase(current_phase, centeroid_count);
                setAggregatedValue(KMEANSPP_PHASE, new LongWritable(next_phase));
                return;
            }

            /** KMeans */
            if (current_phase == PHASE_FINDING_COMPLETE) {
                setAggregatedValue(KMEANSPP_PHASE, new LongWritable(PHASE_RUN_KMEANS));
                return;
            }

            if (current_phase == PHASE_RUN_KMEANS) {
                /** Update the new centeroid aggregators */
                updateCenteroidAggregator();

                /** Check how many datapoints have not been converged */
                long num_not_converged = ((LongWritable)getAggregatedValue(KMEANSPP_NOT_CONVERGED)).get();
                if (num_not_converged == 0) {
                    for (int i = 0; i < numCenteroids; i++) {
                        Vector centeroid = ((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + i)).get();
                        LOG.info("KMeans++: final centeroid " + i + " at " + centeroid);
                    }
                    haltComputation();
                }
                return;
            }
        }

        @Override
        public void initialize() throws InstantiationException, IllegalAccessException { 
            /** Get the user specified number of of centeroids. 
             * At the command line, specify argument using -ca option, for example, -ca kmeanspp.numCenteroids=3. 
             */
            numCenteroids = getConf().getInt(NUM_CENTEROIDS, 2);

            /** register aggregators for master - nodes communication. */
            registerAggregator(KMEANSPP_INIT_CENTEROID_ID, LongWritableOverwriteAggregator.class);
            registerPersistentAggregator(KMEANSPP_CENTEROID_COUNT, LongWritableOverwriteAggregator.class);
            registerPersistentAggregator(KMEANSPP_PHASE, LongWritableOverwriteAggregator.class);
            registerAggregator(KMEANSPP_NOT_CONVERGED, LongSumAggregator.class);

            /** register k aggregators for k centeroids */
            for (int i = 0; i < numCenteroids; i++) {
                LOG.info("KMeans++: Registering aggregator for " + KMEANSPP_CENTEROID + i);
                registerPersistentAggregator(KMEANSPP_CENTEROID + i, VectorWritableOverwriteAggregator.class);
            }
        }
    }
}
