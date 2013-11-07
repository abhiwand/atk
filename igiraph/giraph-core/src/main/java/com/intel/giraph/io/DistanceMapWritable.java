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
 * Vertex value class for average path length computation.
 */

package com.intel.giraph.io;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.HashMap;

/**
 * DistanceMapWritable class
 */
public class DistanceMapWritable implements Writable, Configurable {
    /**
     * Hadoop configuration handle
     */
    private Configuration conf;
    /**
     * HashMap to track the shortest hop counts between source and current vertex.
     */
    private HashMap<Long, Integer> distanceMap;

    /**
     * Constructor
     */
    public DistanceMapWritable() {
        distanceMap = new HashMap<Long, Integer>();
    }

    /**
     * Getter that returns distance map.
     *
     * @return Distance map (HashMap).
     */
    public HashMap<Long, Integer> getDistanceMap() {
        return distanceMap;
    }

    /**
     * Check if the source vertex has been observed.
     *
     * @param source Source vertex id.
     * @return True if source has been observed. Otherwise false.
     */
    public boolean distanceMapContainsKey(long source) {
        return distanceMap.containsKey(source);
    }

    /**
     * Returns the distance from source to current vertex.
     *
     * @param source Source vertex id.
     * @return Distance from source to current vertex.
     */
    public int distanceMapGet(long source) {
        return distanceMap.get(source);
    }

    /**
     * Add source vertex id and associated distance to the HashMap.
     *
     * @param source   Source vertex id.
     * @param distance Distance from source to current vertex.
     */
    public void distanceMapPut(long source, int distance) {
        distanceMap.put(source, distance);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
    }

    @Override
    public void write(DataOutput output) throws IOException {
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
