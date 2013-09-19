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
 * Message class for average path length computation.
 */

package com.intel.giraph.graphstats.averagepathlength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class HopCountWritable implements Writable, Configurable { 

    private Configuration conf;

    /** Source vertex id. */
    private long source_;

    /** Distance from source to the next vertex */
    private int distance_;

    /**
     * Default constructor.
     */
    public HopCountWritable() {
        this.source_ = 0;
        this.distance_ = 0;
    }

    /**
     * Constructor.
     *
     * @param source Source vertex id.
     * @param distance Distance from source vertex id to the next vertex.
     */
    public HopCountWritable(long source, int distance) {
        this.source_ = source;
        this.distance_ = distance;
    }

    /**
     * Returns source vertex id.
     *
     * @return Source vertex id.
     */
    public long getSource() {
        return this.source_;
    }

    /**
     * Returns the distance value.
     *
     * @return Distance.
     */
    public int getDistance() {
      return this.distance_;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.source_ = input.readLong();
        this.distance_ = input.readInt();
    } 
    
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(this.source_);
        output.writeInt(this.distance_);
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
        return this.source_ + "," + this.distance_;
    }
}


