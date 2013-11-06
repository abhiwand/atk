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

/**
 * Message class for average path length computation.
 */

package com.intel.giraph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

/**
 * HopCountWritable class.
 */
public class HopCountWritable implements Writable, Configurable {
    /** Hadoop configuration handle */
    private Configuration conf;
    /** Source vertex id. */
    private long source;
    /** Distance from source to the next vertex */
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
     * @param source Source vertex id.
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
