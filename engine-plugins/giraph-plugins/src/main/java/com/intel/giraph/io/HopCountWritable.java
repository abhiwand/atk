/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

/**
 * Message class for average path length computation.
 */

package com.intel.giraph.io;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * HopCountWritable class.
 */
public class HopCountWritable implements Writable, Configurable {
    /**
     * Hadoop configuration handle
     */
    private Configuration conf;
    /**
     * Source vertex id.
     */
    private long source;
    /**
     * Distance from source to the next vertex
     */
    private int distance;

    /**
     * Default constructor.
     */
    public HopCountWritable() {
        this.source = 0;
        this.distance = 0;
    }

    /**
     * Constructor.
     *
     * @param source   Source vertex id.
     * @param distance Distance from source vertex id to the next vertex.
     */
    public HopCountWritable(long source, int distance) {
        this.source = source;
        this.distance = distance;
    }

    /**
     * Returns source vertex id.
     *
     * @return Source vertex id.
     */
    public long getSource() {
        return this.source;
    }

    /**
     * Returns the distance value.
     *
     * @return Distance.
     */
    public int getDistance() {
        return this.distance;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.source = input.readLong();
        this.distance = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(this.source);
        output.writeInt(this.distance);
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
        return this.source + "," + this.distance;
    }
}
