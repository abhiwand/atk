/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.giraph.algorithms.lbp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import com.intel.mahout.math.TwoVectorWritable;
import com.intel.mahout.math.IdWithVectorWritable;
import com.intel.mahout.math.DoubleWithVectorWritable;

/**
 * Loopy belief propagation on MRF
 */
@Algorithm(
    name = "Loopy belief propagation"
)
public class LoopyBeliefPropagationComputation extends BasicComputation
  <LongWritable, TwoVectorWritable, DoubleWithVectorWritable,
  IdWithVectorWritable> {
  /** Custom argument for number of super steps */
  public static final String MAX_SUPERSTEPS = "lbp.maxSupersteps";
  /** Custom argument for the Ising smoothing parameter */
  public static final String SMOOTHING = "lbp.smoothing";
  /** Custom argument for the convergence threshold */
  public static final String THRESHOLD = "lbp.threshold";
  /** Logger */
  private static final Logger LOG =
    Logger.getLogger(LoopyBeliefPropagationComputation.class);
  /** Number of super steps */
  private int maxSupersteps = 10;
  /** The Ising smoothing parameter */
  private float smoothing = 2f;
  /** The convergence threshold controlling if sending message */
  private float threshold = 0.001f;

  /**
   * initialize vertex and edges
   * @param vertex
   *        vertex of the graph
   */
  private void initializeVertexEdges(Vertex<LongWritable, TwoVectorWritable,
    DoubleWithVectorWritable> vertex) {
    /* normalize prior and posterior */
    Vector prior = vertex.getValue().getPriorVector();
    Vector posterior = vertex.getValue().getPosteriorVector();
    double sum = 0d;
    for (int i = 0; i < prior.size(); i++) {
      double v = prior.getQuick(i);
      if (v < 0d) {
        LOG.error("Vertex ID: " + vertex.getId() +
            " has a negative prior value.");
        System.exit(-1);
      } else if (v < 0.001d) {
        v = 0.001d;
        prior.setQuick(i, v);
      }
      sum += v;
    }
    for (int i = 0; i < prior.size(); i++) {
      posterior.setQuick(i, prior.getQuick(i) / sum);
      prior.setQuick(i, Math.log(posterior.getQuick(i)));
    }
    /* initialize belief */
    for (Edge<LongWritable, DoubleWithVectorWritable> edge :
      vertex.getEdges()) {
      edge.getValue().getVector().assign(0d);
    }
  }

  @Override
  public void compute(Vertex<LongWritable, TwoVectorWritable,
    DoubleWithVectorWritable> vertex, Iterable<IdWithVectorWritable>
    messages) throws IOException {
    long step = getSuperstep();
    if (step == 0) {
      // Set custom parameters
      maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
      smoothing = getConf().getFloat(SMOOTHING, 2f);
      threshold = getConf().getFloat(THRESHOLD, 0.001f);
      // Initialize vertex and edges
      initializeVertexEdges(vertex);
      return;
    }
    // update posterior
    Vector posterior = vertex.getValue().getPriorVector();
    for (IdWithVectorWritable message : messages) {
      posterior = posterior.plus(message.getVector());
    }
    posterior = posterior.plus(-posterior.maxValue());

    if (step < maxSupersteps) {
      IdWithVectorWritable newMessage = new IdWithVectorWritable();
      newMessage.setId(vertex.getId().get());
      // update belief
      Vector belief = vertex.getValue().getPriorVector().clone();
      Vector tempVector = posterior;
      for (Edge<LongWritable, DoubleWithVectorWritable> edge :
        vertex.getEdges()) {
        double weight = edge.getValue().getData();
        Vector oldBelief = edge.getValue().getVector();
        for (IdWithVectorWritable message : messages) {
          if (edge.getTargetVertexId().get() == message.getId()) {
            tempVector = posterior.minus(message.getVector());
            break;
          }
        }
        for (int i = 0; i < tempVector.size(); i++) {
          double sum = 0d;
          for (int j = 0; j < tempVector.size(); j++) {
            sum += Math.exp(tempVector.getQuick(j) +
              (i == j ? 0d : -(smoothing * weight)));
          }
          belief.setQuick(i, sum > 0d ? Math.log(sum) : Double.MIN_VALUE);
        }
        belief = belief.plus(-belief.maxValue());
        double delta = belief.minus(oldBelief).norm(1d) / belief.size();
        if (delta > threshold) {
          edge.getValue().setVector(belief);
          newMessage.setVector(belief);
          sendMessage(edge.getTargetVertexId(), newMessage);
        }
      }
    }
    // normalize posterior
    for (int i = 0; i < posterior.size(); i++) {
      posterior.setQuick(i, Math.exp(posterior.getQuick(i)));
    }
    posterior = posterior.normalize(1d);
    TwoVectorWritable vertexValue = vertex.getValue();
    vertexValue.setPosteriorVector(posterior);

    vertex.voteToHalt();
  }
}
