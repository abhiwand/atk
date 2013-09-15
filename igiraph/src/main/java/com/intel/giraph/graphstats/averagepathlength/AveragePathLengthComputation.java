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

package com.intel.giraph.graphstats.averagepathlength;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.Algorithm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.log4j.Logger;

/**
 * Average path length calculation.
 */
@Algorithm(
    name = "Average path length",
    description = "Finds average of shortest path lengths between all pairs of nodes."
)

public class AveragePathLengthComputation extends BasicComputation
  <LongWritable, DistanceMapWritable, NullWritable, HopCountWritable> {

  /** Logger handler */
  private static final Logger LOG = Logger.getLogger(AveragePathLengthComputation.class);

  /**
   * @brief Flood message to all its direct neighbors with a new distance value.
   *
   * @param vertex Vertex
   * @param source Source vertex ID.
   * @param new_distance Distance from source to the next destination.
   *
   */
  private void floodMessage(
      Vertex<LongWritable, DistanceMapWritable, NullWritable> vertex,
      long source, int new_distance) {

    for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
      sendMessage(edge.getTargetVertexId(), 
                  new HopCountWritable(source, new_distance));
    }
  }

  @Override
  public void compute(Vertex<LongWritable, DistanceMapWritable, NullWritable> vertex,
                      Iterable<HopCountWritable> messages) {
    
    // initial condition - start with sending message to all its neighbors
    if (getSuperstep() == 0) {
      floodMessage(vertex, vertex.getId().get(), 1);
      vertex.voteToHalt();
      return;
    }

    // Process every message received from its direct neighbors
    for (HopCountWritable message : messages) {
      long source = message.getSource();    // source vertex id
      int distance = message.getDistance(); // distnace between source and current verte

      if (source == vertex.getId().get()) {
        continue; // packet returned to the original sender
      } 

      if (vertex.getValue().distanceMapContainsKey(source)) {
        if (vertex.getValue().distanceMapGet(source) > distance) {
          vertex.getValue().distanceMapPut(source, distance);
          floodMessage(vertex, source, distance + 1);
        }
      } else { 
        vertex.getValue().distanceMapPut(source, distance);
        floodMessage(vertex, source, distance + 1);
      }
    }
    vertex.voteToHalt();
  }

  /**
   * Output format for average path length that supports {@link AveragePathLengthComputation} 
   *  - First column: source vertex id
   *  - Second column: the number of destination vertices
   *  - Third column: sum of hop counts to all destinations
   */
  public static class AveragePathLengthComputationOutputFormat extends
      TextVertexOutputFormat<LongWritable, DistanceMapWritable, NullWritable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new AveragePathLengthComputationWriter();
    }

    /**
     * Simple VertexWriter that supports {@link AveragePathLengthComputation}
     */
    public class AveragePathLengthComputationWriter extends TextVertexWriter {
      @Override
      public void writeVertex(
          Vertex<LongWritable, DistanceMapWritable, NullWritable> vertex)
        throws IOException, InterruptedException {

        String vertex_id_str = vertex.getId().toString();
        HashMap<Long, Integer> distance_map = vertex.getValue().getDistanceMap();

        long num_destinations = 0;
        long sum_hop_counts = 0;
        for (Map.Entry<Long, Integer> entry : distance_map.entrySet()) {
          num_destinations++;
          sum_hop_counts += entry.getValue();
        } 
        getRecordWriter().write(
            new Text(vertex_id_str),
            new Text(num_destinations + "\t" + sum_hop_counts));
      }
    }
  }
}
