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

    /** Custom argument for the number of clusters */
    public static final String KMEANSPP_NUM_CENTEROIDS = "kmeanspp.num_centeroids";

    /** aggregator - initial centeriod */
    public static final String KMEANSPP_INIT_CENTEROID_ID = "kmeanspp.agg.init_centeroid_id";
    /** aggregator - number of found centeriods */
    public static final String KMEANSPP_CENTEROID_COUNT = "kmeanspp.agg.centeroid_count";
    /** aggregator - centeriod[0:k-1] */
    public static final String KMEANSPP_CENTEROID = "kmeanspp.agg.centeroid";
    /** aggregator - phases of running KMeans and KMeans++ */
    public static final String KMEANSPP_PHASE = "kmeanspp.agg.phase";
    /** aggregator - total number of not converged datapoints */
    public static final String KMEANSPP_NOT_CONVERGED = "kmeanspp.agg.not_converged";

    /** KMeans++ - phase for datapoints to distribute its vector to centeroid */
    private static final long PHASE_DISTRIBUTE_POSITION = 0;
    /** KMeans++ - phase for centeroid to compute new centeroid */
    private static final long PHASE_COMPUTE_DISTANCE = 1;
    /** KMeans++ - phase for the KMeans++ algorithm to prepare for running KMeans. */
    private static final long PHASE_FINDING_COMPLETE = 2;
    /** KMeans++ - phase for running KMeans algorithm. */
    private static final long PHASE_RUN_KMEANS = 3;
    /** Number of super steps */
    private static final int MAX_SUPERSTEPS = 1600;

    /** Number of centeroids */
    private static int NUM_CENTEROIDS = 2;
    /** Number of datapoints */
    private static long NUM_DATAPOINTS = 0;

    /** Class logger */
    private static Logger LOG = Logger.getLogger(KMeansppComputation.class);

    /** Random number generator. */
    private Random randGen = new Random();


    @Override
    public void preSuperstep() {
        NUM_CENTEROIDS = getConf().getInt(KMEANSPP_NUM_CENTEROIDS, 2);

        if (getSuperstep() == 0) {
            NUM_DATAPOINTS = getTotalNumVertices();
        }
    }

    /**
     * Adding a centeroid node that computes the distances and selects the next centeroid.
     *
     * @param newVid Centeroid node's vertex id.
     * @param centeroid KMeans++'s randomly selected initial centeroid.
     */
    private void addCenteroid(long newVid, Vector centeroid) throws IOException {
        LOG.info("KMeans++: adding centeroid node to compute centeroid with the vid=" + newVid);
        LongWritable centeroidVid = new LongWritable(newVid);
        addVertexRequest(centeroidVid, new IdWithVectorWritable(-1, centeroid));

        LOG.info("KMeans++: adding edges between the centeroid and datapoints.");
        for (long i = 0; i < NUM_DATAPOINTS; i++) {
            LongWritable vid = new LongWritable(i);
            addEdgeRequest(vid, EdgeFactory.create(centeroidVid, NullWritable.get()));
            addEdgeRequest(centeroidVid, EdgeFactory.create(vid, NullWritable.get()));
        }
    }

    /**
     * Computes the shortest distance between a datapoint and centeroids.
     *
     * @param centeroidVecList List of centeroid vectors.
     * @param vec2 Datapoint vector.
     *
     * @return shortest distance.
     */
    private double computeShortestDistance(List<Vector> centeroidVecList, Vector vec2) {
        double currentDistance = Double.POSITIVE_INFINITY;
        for (Vector vec1 : centeroidVecList) {
            double distance = vec1.getDistanceSquared(vec2);
            LOG.debug("KMeans++: vec1=" + vec1 + ", vec2=" + vec2 + ", dist=" + distance);
            if (distance < currentDistance) {
                currentDistance = distance;
            }
        }
        LOG.debug("KMeans++: shortest distnce = " + currentDistance);
        return currentDistance;
    }

    /**
     * Check if the current vertex is master centeroid.
     *
     * @param myVid Vertex ID.
     *
     * @return true if the vertex is master centeroid. false otherwise.
     */
    private boolean isCenteroid(long myVid) {
        return !(myVid < NUM_DATAPOINTS);
    }

    /**
     * Check if the current vertex is datapoint.
     *
     * @param myVid Vertex ID.
     *
     * @return true if the vertex is datapoint. false otherwise.
     */
    private boolean isDatapoint(long myVid) {
        return myVid < NUM_DATAPOINTS;
    }

    /**
     * Broadcast centeroid vector to datapoints.
     *
     * @param vid Vertex ID of centeroid.
     * @param vec Centeroid vector.
     * @param edges Edges to datapoints.
     */
    private void broadcastCenteroid(long vid, Vector vec, Iterable<Edge<LongWritable, NullWritable>> edges) {
        for (Edge<LongWritable, NullWritable> edge : edges) {
            LOG.debug("KMeans++: the centeroid " + vid + " sends its vector to datapoints.");
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
        if (v1.size() != v2.size()) {
            throw new IllegalArgumentException("addTwoVector: sizes of two vectors are different.");
        }
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
     * @param messages Messages received from datapoints.
     *
     * @return The new centeroid vector.
     */
    private Vector computeCenteroid(Iterable<IdWithVectorWritable> messages) {
        boolean init = false;
        Vector resultVec = new DenseVector();
        long numMessages = 0;
        for (IdWithVectorWritable message : messages) {
            numMessages++;
            Vector v = message.getVector();
            LOG.debug("KMeans++: computeCenteroid v = " + v);
            if (!init) {
                resultVec = v.clone();
                LOG.debug("KMeans++: computeCenteroid resultVec + v = " + resultVec);
                init = true;
                continue;
            }
            //resultVec = addTwoVector(resultVec, v);
            resultVec = resultVec.plus(v);
            LOG.debug("KMeans++: computeCenteroid resultVec + v = " + resultVec);
        }

        int dimension = resultVec.size();
        for (int i = 0; i < dimension; i++) {
            resultVec.set(i, resultVec.get(i) / (double) numMessages);
        }
        return resultVec;
    }

    /**
     * Returns the array of centeroid vectors.
     *
     * @return Centeroid vectors in array.
     */
    private Vector[] getCenteroidVectorArray() {
        Vector[] centeroidVec = new Vector[NUM_CENTEROIDS];
        for (int i = 0; i < NUM_CENTEROIDS; i++) {
            centeroidVec[i] = ((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + i)).get();
        }
        return centeroidVec;
    }

    /**
     * Returns the centeroid vector of centeroid.
     *
     * @param centeroidId Centeroid ID.
     *
     * @return Conteroid vector.
     */
    private Vector getCenteroidVector(long centeroidId) {
        return ((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + centeroidId)).get();
    }

    @Override
    /**
    * KMeans++ vertex computation routine.
     *
     * @param vertex Vertex instance.
     * @param messages Messages that the vertex received.
     */
    public void compute(Vertex<LongWritable, IdWithVectorWritable, NullWritable> vertex,
        Iterable<IdWithVectorWritable> messages) throws IOException {

        long step = getSuperstep();
        long vid = vertex.getId().get();
        long phase = ((LongWritable) getAggregatedValue(KMEANSPP_PHASE)).get();

        /** number of centeroid that has been selected by kmeans++ */
        long centeroidCount = ((LongWritable) getAggregatedValue(KMEANSPP_CENTEROID_COUNT)).get();

        // initial random centeroid
        if (step == 0) {
            // center seletected -> selected center create centeroid.
            long initialCenteroid = ((LongWritable)
                getAggregatedValue(KMEANSPP_INIT_CENTEROID_ID)).get() % NUM_DATAPOINTS;

            if (isDatapoint(vid)) {
                if (vid == initialCenteroid) {
                    LOG.info("KMeans++: initial centeroid = " + initialCenteroid);
                    LOG.info("KMeans++: initial centeroid at " + vertex.getValue().getVector());
                    addCenteroid(NUM_DATAPOINTS + centeroidCount, vertex.getValue().getVector());
                    aggregate(KMEANSPP_CENTEROID + centeroidCount,
                        new VectorWritable(new DenseVector(vertex.getValue().getVector())));
                }
            }
            vertex.getValue().setData(Long.MAX_VALUE);
            return;
        }

        // datapoints distribute its position vector to centeroid
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

        // centeroid computes distance
        if (phase == PHASE_COMPUTE_DISTANCE) {
            if (isDatapoint(vid)) {
                // Datapoints has nothing to do in this phase.
                return;
            }

            LOG.info("KMeans++: centeroid" + vid + " computes the shortest distance.");
            List<Vector> centeroidVec = new ArrayList<Vector>();
            for (long i = 0; i < centeroidCount; i++) {
                Vector centeroid = ((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + i)).get();
                centeroidVec.add(centeroid);
            }

            // finding the closest centeroid from this datapont and calculating the distance.
            List<Double> distanceList = new ArrayList<Double>();
            double sumDistance = 0.0;
            for (IdWithVectorWritable message : messages) {
                double distance = computeShortestDistance(centeroidVec, message.getVector());
                sumDistance += distance;
                distanceList.add(sumDistance);
            }

            int idx = 0;
            double prob = sumDistance * randGen.nextDouble();
            for (IdWithVectorWritable message : messages) {
                if (prob < distanceList.get(idx)) {
                    LOG.info("KMeans++: found centeroid" + centeroidCount + " at " + message.getVector());
                    aggregate(KMEANSPP_CENTEROID + centeroidCount, new VectorWritable(message.getVector()));
                    vertex.setValue(new IdWithVectorWritable(-1, message.getVector()));
                    break;
                }
                idx++;
            }
            return;
        }

        // KMeans++ initial centeroids finding is done - now performing preparation for running KMeans algorithm
        if (phase == PHASE_FINDING_COMPLETE) {
            if (isDatapoint(vid)) {
                // put datapoints into sleep and wake them when all the centeroids are
                // getting active in the next phase (PHASE_RUN_KMEANS).
                vertex.voteToHalt();
                return;
            }

            // master centeroid preparing for the KMeans by creating new centeroids found in the previous steps.
            for (long centeroidId = NUM_DATAPOINTS; centeroidId < (NUM_DATAPOINTS + NUM_CENTEROIDS); centeroidId++) {
                Vector centeroidVec = getCenteroidVector(centeroidId - NUM_DATAPOINTS);
                LOG.info("KMeans++: new centeroid " + centeroidId + " with vector " + centeroidVec);
                if (centeroidId == NUM_DATAPOINTS) {
                    // master centeroid is setting itself with a centeroid vector
                    vertex.setValue(new IdWithVectorWritable(-1, centeroidVec));
                } else {
                    // master centeroid creates the reset of (k-1) centeroids and set them with centeroid vectors
                    addCenteroid(centeroidId, centeroidVec);
                }
            }
            return;
        }

        if (phase == PHASE_RUN_KMEANS) {
            if (isCenteroid(vid)) {
                LOG.info("KMeans++: the centeroid " + vid + " calculates the new centeroid.");
                Vector newCenteroidVec = computeCenteroid(messages);

                if (newCenteroidVec.size() > 0) {
                    // the new centeroid has been calculated - broadcast the new centeroid!
                    LOG.info("KMeans++: broadcast the new centeroid " + newCenteroidVec);

                    vertex.setValue(new IdWithVectorWritable(-1, newCenteroidVec));

                    broadcastCenteroid(vid, newCenteroidVec, vertex.getEdges());
                } else {
                    // centeroid starts KMeans with broadcasting its location to datapoints
                    LOG.info("KMeans++: broadcast the initial centeroid " + vertex.getValue().getVector());
                    broadcastCenteroid(vid, vertex.getValue().getVector(), vertex.getEdges());
                }

                /** set new centeroid */
                long centeroidId = vid - NUM_DATAPOINTS;
                LOG.info("KMeans++: aggregate to " + KMEANSPP_CENTEROID + centeroidId);
                aggregate(KMEANSPP_CENTEROID + centeroidId, new VectorWritable(newCenteroidVec));

                // convergence cannot happen yet
                aggregate(KMEANSPP_NOT_CONVERGED, new LongWritable(1));

                vertex.voteToHalt();
                return;
            }

            if (isDatapoint(vid)) {
                long newCenteroid = Long.MAX_VALUE;
                double shortestDistance = Double.POSITIVE_INFINITY;
                for (IdWithVectorWritable message : messages) {
                    double dist = (message.getVector()).getDistanceSquared(vertex.getValue().getVector());
                    if (dist < shortestDistance) {
                        shortestDistance = dist;
                        newCenteroid = message.getData();
                    }
                }

                long closestCenteroid = vertex.getValue().getData();
                LOG.debug("KMeans++: old = " + closestCenteroid + ", new = " + newCenteroid);
                if (closestCenteroid != newCenteroid) {
                    closestCenteroid = newCenteroid;
                    vertex.getValue().setData(closestCenteroid);
                    aggregate(KMEANSPP_NOT_CONVERGED, new LongWritable(1));
                }
                sendMessage(new LongWritable(closestCenteroid),
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
         *
         * @param currentPhase Current phase.
         * @param centeroidCount The number of found centeroids.
         *
        *  @return The next phase.
         */
        private long getNextPhase(long currentPhase, long centeroidCount) {
            if (centeroidCount < NUM_CENTEROIDS) {
                if (currentPhase == PHASE_DISTRIBUTE_POSITION) {
                    return PHASE_COMPUTE_DISTANCE;
                } else {
                    return PHASE_DISTRIBUTE_POSITION;
                }
            }
            return PHASE_FINDING_COMPLETE;
        }

        /**
         * KMeans++ master computation routine.
         */
        @Override
        public void compute() {
            long step = getSuperstep();

            if (step == 0) {
                /** selects initial centroid uniformly at random */
                Random rand = new Random();
                long initialCenteroid = Math.abs(rand.nextLong());
                LOG.info("KMeans++: random centeroid vid = " + initialCenteroid);

                /** initialize aggregators with initial values */
                setAggregatedValue(KMEANSPP_INIT_CENTEROID_ID, new LongWritable(initialCenteroid));
                setAggregatedValue(KMEANSPP_CENTEROID_COUNT, new LongWritable(0));
                setAggregatedValue(KMEANSPP_PHASE, new LongWritable(-1));
                setAggregatedValue(KMEANSPP_NOT_CONVERGED , new LongWritable(0));
                return;
            }

            /** KMeans++ phase */
            long currentPhase = ((LongWritable) getAggregatedValue(KMEANSPP_PHASE)).get();

            // KMeans++ finding initial centeroids
            if (currentPhase < PHASE_FINDING_COMPLETE) {
                // update the aggregators with current centeroids
                long centeroidCount;
                for (centeroidCount = 0; centeroidCount < NUM_CENTEROIDS; centeroidCount++) {
                    Vector centeroid = ((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + centeroidCount)).get();
                    if (centeroid.size() > 0) {
                        LOG.info("KMeans++: centeroid " + centeroidCount + " at " + centeroid);
                        setAggregatedValue(KMEANSPP_CENTEROID + centeroidCount, new VectorWritable(centeroid));
                        continue;
                    }
                    break;
                }

                // update the number of found centeroids
                setAggregatedValue(KMEANSPP_CENTEROID_COUNT, new LongWritable(centeroidCount));

                // update the next centeroid-finding-phase based on the number of already found centeroids.
                long nextPhase = getNextPhase(currentPhase, centeroidCount);
                setAggregatedValue(KMEANSPP_PHASE, new LongWritable(nextPhase));
                return;
            }

            // KMeans starts here.
            if (currentPhase == PHASE_FINDING_COMPLETE) {
                setAggregatedValue(KMEANSPP_PHASE, new LongWritable(PHASE_RUN_KMEANS));
                return;
            }

            if (currentPhase == PHASE_RUN_KMEANS) {
                /** Check how many datapoints have not been converged */
                long numNotConvergedDatapoints = ((LongWritable) getAggregatedValue(KMEANSPP_NOT_CONVERGED)).get();
                if (numNotConvergedDatapoints == 0) {
                    for (int i = 0; i < NUM_CENTEROIDS; i++) {
                        Vector centeroid = ((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + i)).get();
                        LOG.info("KMeans++: final centeroid " + i + " at " + centeroid);
                    }
                    haltComputation();
                }
                return;
            }
        }

        /**
         * MasterCompute initialization.
         */
        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            // Get the user specified number of of centeroids.
            // At the command line, specify argument using -ca option, for example, -ca kmeanspp.num_centeroids=3.
            NUM_CENTEROIDS = getConf().getInt(KMEANSPP_NUM_CENTEROIDS, 2);

            // register aggregators for master - nodes communication.
            registerAggregator(KMEANSPP_INIT_CENTEROID_ID, LongWritableOverwriteAggregator.class);
            registerPersistentAggregator(KMEANSPP_CENTEROID_COUNT, LongWritableOverwriteAggregator.class);
            registerPersistentAggregator(KMEANSPP_PHASE, LongWritableOverwriteAggregator.class);
            registerAggregator(KMEANSPP_NOT_CONVERGED, LongSumAggregator.class);

            // register k aggregators for k centeroids
            for (int i = 0; i < NUM_CENTEROIDS; i++) {
                LOG.info("KMeans++: Registering aggregator for " + KMEANSPP_CENTEROID + i);
                registerPersistentAggregator(KMEANSPP_CENTEROID + i, VectorWritableOverwriteAggregator.class);
            }
        }
    }
}
