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

import org.apache.hadoop.io.LongWritable;
import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import com.intel.mahout.math.TwoVectorWritable;
import org.apache.mahout.math.Vector;

import com.intel.mahout.math.IdWithVectorWritable;
import com.intel.mahout.math.DoubleWithVectorWritable;

/**
 * Label Propagation on Gaussian Random Fields
 * The algorithm presented in:
 * X. Zhu and Z. Ghahramani. Learning from labeled and unlabeled data with
 * label propagation. Technical Report CMU-CALD-02-107, CMU, 2002.
 */
@Algorithm(
    name = "Label Propagation on Gaussian Random Fields"
)
public class LabelPropagationComputation extends BasicComputation<LongWritable, TwoVectorWritable,
    DoubleWithVectorWritable, IdWithVectorWritable> {
    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "lp.maxSupersteps";
    /**
     * Custom argument for tradeoff parameter: lambda
     * f = (1-lambda)Pf + lambda*h
     */
    public static final String LAMBDA = "lp.lambda";
    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "lp.convergenceThreshold";
    /**
     * Custom argument for the anchor threshold [0, 1]
     * the vertices whose normalized values are greater than
     * this threshold will not be updated.
     * */
    public static final String ANCHOR_THRESHOLD = "lp.anchorThreshold";

    /** Number of super steps */
    private int maxSupersteps = 10;
    /** The tradeoff parameter between prior and posterior */
    private float lambda = 0f;
    /** The convergence threshold controlling if sending message */
    private float convergenceThreshold = 0.001f;
    /** The anchor threshold controlling if updating a vertex */
    private float anchorThreshold = 1f;

    @Override
    public void preSuperstep() {
        // Set custom parameters
        maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
        lambda = getConf().getFloat(LAMBDA, 0f);
        if (lambda < 0 || lambda > 1) {
            throw new IllegalArgumentException("Tradeoff parameter lambda should be in the range of [0, 1].");
        }
        convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
        anchorThreshold = getConf().getFloat(ANCHOR_THRESHOLD, 1f);
    }

    /**
     * initialize vertex and edges
     *
     * @param vertex of the graph
     */
    private void initializeVertexEdges(Vertex<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> vertex) {
        // normalize prior and initialize posterior
        Vector prior = vertex.getValue().getPriorVector();
        if (prior.minValue() < 0d) {
            throw new IllegalArgumentException("Vertex ID: " + vertex.getId() + " has negative prior value.");
        }
        prior = prior.normalize(1d);
        vertex.getValue().setPriorVector(prior);
        vertex.getValue().setPosteriorVector(prior.clone());
        // normalize edge weight
        double sum = 0d;
        for (Edge<LongWritable, DoubleWithVectorWritable> edge : vertex.getEdges()) {
            double weight = edge.getValue().getData();
            if (weight <= 0d) {
                throw new IllegalArgumentException("Vertex ID: " + vertex.getId() +
                    " has an edge with negative weight value.");
            }
            sum += weight;
        }
        for (Edge<LongWritable, DoubleWithVectorWritable> edge : vertex.getMutableEdges()) {
            edge.getValue().setData(edge.getValue().getData() / sum);
            edge.getValue().getVector().assign(0d);
        }
        // send out messages
        IdWithVectorWritable newMessage = new IdWithVectorWritable();
        newMessage.setData(vertex.getId().get());
        newMessage.setVector(vertex.getValue().getPriorVector());
        sendMessageToAllEdges(vertex, newMessage);
    }

    @Override
    public void compute(Vertex<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> vertex,
        Iterable<IdWithVectorWritable> messages) throws IOException {
        long step = getSuperstep();
        if (step == 0) {
            initializeVertexEdges(vertex);
            vertex.voteToHalt();
            return;
        }

        // if it's an anchor vertex, no update is necessary
        if (vertex.getValue().getPriorVector().maxValue() >= anchorThreshold) {
            vertex.voteToHalt();
            return;
        }

        if (step < maxSupersteps) {
            // Update edge vector value from message
            HashMap<Long, Vector> map = new HashMap<Long, Vector>();
            for (IdWithVectorWritable message : messages) {
                map.put(message.getData(), message.getVector());
            }
            if (map.size() > 0) {
                for (Edge<LongWritable, DoubleWithVectorWritable> edge : vertex.getMutableEdges()) {
                    long id = edge.getTargetVertexId().get();
                    if (map.containsKey(id)) {
                        edge.getValue().setVector(map.get(id));
                    }
                }
            }
            // update belief
            Vector oldBelief = vertex.getValue().getPosteriorVector();
            Vector belief = oldBelief.clone().assign(0d);
            for (Edge<LongWritable, DoubleWithVectorWritable> edge : vertex.getEdges()) {
                double weight = edge.getValue().getData();
                Vector tempVector = edge.getValue().getVector();
                belief = belief.plus(tempVector.times(weight));
            }
            Vector prior = vertex.getValue().getPriorVector();
            belief = belief.times(1 - lambda).plus(prior.times(lambda));
            double delta = belief.minus(oldBelief).norm(1d) / belief.size();
            if (delta > convergenceThreshold) {
                vertex.getValue().setPosteriorVector(belief);
                IdWithVectorWritable newMessage = new IdWithVectorWritable();
                newMessage.setData(vertex.getId().get());
                newMessage.setVector(belief);
                sendMessageToAllEdges(vertex, newMessage);
            }
        }

        vertex.voteToHalt();
    }

}
