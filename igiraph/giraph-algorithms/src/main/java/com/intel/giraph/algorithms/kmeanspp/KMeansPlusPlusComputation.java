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

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.DenseVector;

import org.apache.log4j.Logger;

import com.intel.mahout.math.IdWithVectorWritable;
import com.intel.giraph.aggregators.VectorWritableOverwriteAggregator;
import com.intel.giraph.aggregators.LongWritableOverwriteAggregator;

/**
 * KMeans++ algorithm.
 */
@Algorithm(
  name = "KMeans++"
)
public class KMeansPlusPlusComputation extends BasicComputation<LongWritable, VectorWritable,
  NullWritable, IdWithVectorWritable> {

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

  /** Aggregator to exchange values between the master to workers */
  public static final String KMEANSPP_INIT_CENTEROID_ID = "kmeanspp.agg.init_centeroid_id";
  public static final String KMEANSPP_CENTEROID_COUNT = "kmeanspp.agg.centeroid_count";
  public static final String KMEANSPP_CENTEROID = "centeroid";
  public static final String KMEANSPP_SUM_DISTANCE = "kmeanspp.agg.sum_distance";
  public static final String KMEANSPP_PHASE = "kmeanspp.agg.phase";

  /** Random number generator.
   * TODO: we might need fixed seed for QA purpose. */
  Random rand_gen = new Random();

  /** Class logger */
  private static Logger LOG = Logger.getLogger(KMeansPlusPlusComputation.class);

  @Override
  public void preSuperstep() {
    maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
    convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
    
    if (getSuperstep() == 0) {
      numDatapoints = getTotalNumVertices();
    }
  }

  /**
   * Adding a master node that computes the initial centeroids.
   *
   * @param master_vid Master node's vertex id.
   * @param centeroid KMeans++'s randomly selected initial centeroid.
   */
  private void addMasterCenteroid(long master_vid, Vector centeroid) throws 
      IOException {
    LOG.info("KMeans++: adding master node to compute centeroid.");
    LongWritable centeroid_vid = new LongWritable(master_vid);
    addVertexRequest(centeroid_vid, new VectorWritable(centeroid));

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
      LOG.debug("vec1=" + vec1 + ", vec2=" + vec2 + ", dist=" + distance);
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

  @Override
  public void compute(Vertex<LongWritable, VectorWritable, NullWritable> vertex,
    Iterable<IdWithVectorWritable> messages) throws IOException {
    long step = getSuperstep(); 
    long vid = vertex.getId().get();
    long phase = ((LongWritable) getAggregatedValue(KMEANSPP_PHASE)).get();

    /** number of centeroid that has been selected by kmeans++ */
    long centeroid_count = ((LongWritable) 
        getAggregatedValue(KMEANSPP_CENTEROID_COUNT)).get();

    /** initial random centeroid */
    if (step == 0) { 
      /* center seletected -> selected center create centeroid. */
      long init_centeroid = ((LongWritable) 
          getAggregatedValue(KMEANSPP_INIT_CENTEROID_ID)).get() % numDatapoints;

      if (isDatapoint(vid)) {
        if (vid == init_centeroid) {
          LOG.info("KMeans++: initial centeroid = " + init_centeroid);
          LOG.info("KMeans++: initial centeroid at " + vertex.getValue().get());
          addMasterCenteroid(numDatapoints + centeroid_count, vertex.getValue().get());

          aggregate(KMEANSPP_CENTEROID + centeroid_count, new VectorWritable(new DenseVector(vertex.getValue().get())));
        }
      }
      return;
    }

    /** datapoints only */
    if (phase == PHASE_DISTRIBUTE_POSITION) {
      if (isDatapoint(vid)) {
        for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
          LOG.debug("KMeans++: datapoint " + vid + " sends its location to centeroid");
          sendMessage(edge.getTargetVertexId(), 
                      new IdWithVectorWritable(vid, vertex.getValue().get()));
        }
      }
      return;
    }

    /** centeroid only */
    if (phase == PHASE_COMPUTE_DISTANCE) {
      if (isCenteroid(vid)) {
        LOG.info("KMeans++: centeroid" + vid + " computes the shortest distance.");
        List<Vector> centeroid_vec = new ArrayList<Vector>();
        for (long centeroid_id = 0; centeroid_id < centeroid_count; centeroid_id++) {
          Vector centeroid = ((VectorWritable) getAggregatedValue(KMEANSPP_CENTEROID + centeroid_id)).get();
          centeroid_vec.add(centeroid);
        }

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
            vertex.setValue(new VectorWritable(message.getVector()));
            break;
          }
          idx++;
        }
        vertex.voteToHalt();
      }
      return;
    }

    if (phase == PHASE_FINDING_COMPLETE) {
      vertex.voteToHalt();
    }
  }

  /**
   * MasterCompute used with {@link KMeansPlusPlusComputation}.
   */
  public static class KMeansPlusPlusMasterCompute extends 
      DefaultMasterCompute {

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
      }

      /** KMeans++ phase */
      long current_phase = ((LongWritable) getAggregatedValue(KMEANSPP_PHASE)).get();

      /** KMeans++ finding initial centeroids */
      if (current_phase < PHASE_FINDING_COMPLETE) {
        /** update the aggregators with current centeroids */
        long centeroid_count;
        for (centeroid_count = 0; centeroid_count < numCenteroids; centeroid_count++) {
          Vector centeroid = ((VectorWritable) 
            getAggregatedValue(KMEANSPP_CENTEROID + centeroid_count)).get();
          if (centeroid.size() > 0) {
            LOG.info("KMeans++: centeroid " + centeroid_count + " at " + centeroid);
            setAggregatedValue(KMEANSPP_CENTEROID + centeroid_count, 
                               new VectorWritable(centeroid));
            continue;
          }
          break;
        }

        /** update the number of found centeroids */
        setAggregatedValue(KMEANSPP_CENTEROID_COUNT, new LongWritable(centeroid_count));

        /** update the next centeroid-finding-phase based on the number of
         *  already found centeroids.
         */
        long next_phase = getNextPhase(current_phase, centeroid_count);
        setAggregatedValue(KMEANSPP_PHASE, new LongWritable(next_phase));
      }
    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        
      /** Get the user specified number of of centeroids. At the command line,
       * specify argument using -ca option, for example, -ca kmeanspp.numCenteroids=3.
       */
      numCenteroids = getConf().getInt(NUM_CENTEROIDS, 2);

      /** register aggregators for master - nodes communication. */
      registerAggregator(KMEANSPP_INIT_CENTEROID_ID, 
                         LongWritableOverwriteAggregator.class);
      registerPersistentAggregator(KMEANSPP_CENTEROID_COUNT, 
                                   LongWritableOverwriteAggregator.class);
      registerPersistentAggregator(KMEANSPP_PHASE, 
                                   LongWritableOverwriteAggregator.class);

      /** register k aggregators for k centeroids */
      for (int i = 0; i < numCenteroids; i++) {
        LOG.info("KMeans++: Registering aggregator for " + KMEANSPP_CENTEROID + i);
        registerPersistentAggregator(KMEANSPP_CENTEROID + i, 
                                     VectorWritableOverwriteAggregator.class);
      }
    }
  }
}
