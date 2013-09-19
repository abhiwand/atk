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

/**
 * Vertex value class for average path length computation.
 */

package com.intel.giraph.graphstats.averagepathlength;

import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;


public class DistanceMapWritable implements Writable, Configurable { 

  private Configuration conf;

  /** HashMap to track the shortest hop counts between source and current vertex. */
  private HashMap<Long, Integer> distance_map_; 

  /** Constructor */
  public DistanceMapWritable() {
    distance_map_ = new HashMap<Long, Integer>();
  }

  /**
   * Getter that returns distance map.
   *
   * @return Distance map (HashMap).
   */
  public HashMap<Long, Integer> getDistanceMap() {
    return distance_map_;
  }

  /**
   * Check if the source vertex has been observed.
   *
   * @param source Source vertex id.
   *
   * @return True if source has been observed. Otherwise false.
   */
  public boolean distanceMapContainsKey(long source) {
    return distance_map_.containsKey(source);
  }

  /**
   * Returns the distance from source to current vertex.
   *
   * @param source Source vertex id.
   *
   * @return Distance from source to current vertex.
   */
  public int distanceMapGet(long source) {
    return distance_map_.get(source);
  }

  /**
   * Add source vertex id and associated distance to the HashMap.
   *
   * @param source Source vertex id.
   * @param distance Distance from source to current vertex.
   *
   */
  public void distanceMapPut(long source, int distance) {
    distance_map_.put(source, distance);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    // TODO: check if it's needed.
  } 
  
  @Override
  public void write(DataOutput output) throws IOException {
    // TODO: check if it's needed.
  } 
  
  @Override
  public Configuration getConf() {
      return this.conf;
  } 
  
  @Override
  public void setConf(Configuration conf) {
      this.conf = conf;
  } 
  
  @Override
  public String toString() {
      return "";
  }
}
