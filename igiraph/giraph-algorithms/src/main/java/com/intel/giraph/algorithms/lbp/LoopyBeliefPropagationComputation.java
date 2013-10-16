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

package com.intel.giraph.algorithms.lbp;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.mahout.math.Vector;

import com.intel.mahout.math.TwoVectorWritable;
import com.intel.mahout.math.IdWithVectorWritable;
import com.intel.mahout.math.DoubleWithTwoVectorWritable;

/**
 * Loopy belief propagation on MRF
 */
@Algorithm(
    name = "Loopy belief propagation"
)
public class LoopyBeliefPropagationComputation extends BasicComputation<LongWritable, TwoVectorWritable,
    DoubleWithTwoVectorWritable, IdWithVectorWritable> {
    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "lbp.maxSupersteps";
    /** Custom argument for the Ising smoothing parameter */
    public static final String SMOOTHING = "lbp.smoothing";
    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "lbp.convergenceThreshold";
    /**
     * Custom argument for the anchor threshold [0, 1]
     * the vertices whose normalized prior values are greater than
     * this threshold will not be updated.
     * */
    public static final String ANCHOR_THRESHOLD = "lbp.anchorThreshold";
    /** Constant value for minimum prior value */
    public static final double MIN_PRIOR_VALUE = 0.001d;

    /** Number of super steps */
    private int maxSupersteps = 10;
    /** The Ising smoothing parameter */
    private float smoothing = 2f;
    /** The convergence threshold controlling if sending message */
    private float convergenceThreshold = 0.001f;
    /** The anchor threshold controlling if updating a vertex */
    private float anchorThreshold = 1f;

    @Override
    public void preSuperstep() {
        // Set custom parameters
        maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
        smoothing = getConf().getFloat(SMOOTHING, 2f);
        convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
        anchorThreshold = getConf().getFloat(ANCHOR_THRESHOLD, 1f);
        anchorThreshold = (float) Math.log(anchorThreshold);
    }

    /**
     * initialize vertex and edges
     *
     * @param vertex of the graph
     */
    private void initializeVertexEdges(Vertex<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> vertex) {
        // normalize prior and posterior
        Vector prior = vertex.getValue().getPriorVector();
        Vector posterior = vertex.getValue().getPosteriorVector();
        double sum = 0d;
        for (int i = 0; i < prior.size(); i++) {
            double v = prior.getQuick(i);
            if (v < 0d) {
                throw new IllegalArgumentException("Vertex ID: " + vertex.getId() + " has negative prior value.");
            } else if (v < MIN_PRIOR_VALUE) {
                v = MIN_PRIOR_VALUE;
                prior.setQuick(i, v);
            }
            sum += v;
        }
        for (int i = 0; i < prior.size(); i++) {
            posterior.setQuick(i, prior.getQuick(i) / sum);
            prior.setQuick(i, Math.log(posterior.getQuick(i)));
        }
        // initialize belief
        for (Edge<LongWritable, DoubleWithTwoVectorWritable> edge : vertex.getMutableEdges()) {
            edge.getValue().getVectorOut().assign(0d);
            edge.getValue().getVectorIn().assign(0d);
        }
    }

    @Override
    public void compute(Vertex<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> vertex,
        Iterable<IdWithVectorWritable> messages) throws IOException {
        long step = getSuperstep();
        if (step == 0) {
            initializeVertexEdges(vertex);
            return;
        }
        // Update vectorIn from message
        HashMap<Long, Vector> map = new HashMap<Long, Vector>();
        for (IdWithVectorWritable message : messages) {
            map.put(message.getData(), message.getVector());
        }
        if (map.size() > 0) {
            for (Edge<LongWritable, DoubleWithTwoVectorWritable> edge : vertex.getMutableEdges()) {
                long id = edge.getTargetVertexId().get();
                if (map.containsKey(id)) {
                    edge.getValue().setVectorIn(map.get(id));
                }
            }
        }
        // Update posterior
        Vector posterior = vertex.getValue().getPriorVector();
        for (Edge<LongWritable, DoubleWithTwoVectorWritable> edge : vertex.getEdges()) {
            posterior = posterior.plus(edge.getValue().getVectorIn());
        }
        posterior = posterior.plus(-posterior.maxValue());

        if (step < maxSupersteps) {
            IdWithVectorWritable newMessage = new IdWithVectorWritable();
            newMessage.setData(vertex.getId().get());
            // update belief
            Vector belief = vertex.getValue().getPriorVector().clone();
            Vector tempVector = posterior;
            for (Edge<LongWritable, DoubleWithTwoVectorWritable> edge : vertex.getMutableEdges()) {
                double weight = edge.getValue().getData();
                Vector oldBelief = edge.getValue().getVectorOut();
                tempVector = posterior.minus(edge.getValue().getVectorIn());
                for (int i = 0; i < tempVector.size(); i++) {
                    double sum = 0d;
                    for (int j = 0; j < tempVector.size(); j++) {
                        sum += Math.exp(tempVector.getQuick(j) + (i == j ? 0d : -(smoothing * weight)));
                    }
                    belief.setQuick(i, sum > 0d ? Math.log(sum) : Double.MIN_VALUE);
                }
                belief = belief.plus(-belief.maxValue());
                double delta = belief.minus(oldBelief).norm(1d) / belief.size();
                if (delta > convergenceThreshold) {
                    edge.getValue().setVectorOut(belief);
                    newMessage.setVector(belief);
                    sendMessage(edge.getTargetVertexId(), newMessage);
                }
            }
        }
        // update posterior if this isn't an anchor vertex
        if (vertex.getValue().getPriorVector().maxValue() < anchorThreshold) {
            // normalize posterior
            for (int i = 0; i < posterior.size(); i++) {
                posterior.setQuick(i, Math.exp(posterior.getQuick(i)));
            }
            posterior = posterior.normalize(1d);
            TwoVectorWritable vertexValue = vertex.getValue();
            vertexValue.setPosteriorVector(posterior);
        }

        vertex.voteToHalt();
    }

}
