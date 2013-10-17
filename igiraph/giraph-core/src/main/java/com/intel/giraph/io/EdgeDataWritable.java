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

package com.intel.giraph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Writable to handle serialization of the fields associated with edge data
 */
public class EdgeDataWritable implements Writable {

    /** the edge type supported by this vertex */
    public enum EdgeType { TRAIN, VALIDATE, TEST, NONE };

    /** the weight value at this edge */
    private double weight = 0d;

    /** the type of this vertex */
    private EdgeType type = EdgeType.NONE;

    /**
     * Default constructor
     */
    public EdgeDataWritable() {
    }

    /**
     * Constructor
     *
     * @param type from EdgeType
     * @param weight of type double
     */
    public EdgeDataWritable(EdgeType type, double weight) {
        this.type = type;
        this.weight = weight;
    }

    /**
     * Setter
     *
     * @param type of type EdgeType
     */
    public void setType(EdgeType type) {
        this.type = type;
    }

    /**
     * Getter
     *
     * @return type of type EdgeType
     */
    public EdgeType getType() {
        return type;
    }

    /**
     * Getter
     *
     * @return weight of type double
     */
    public double getWeight() {
        return weight;
    }

    /**
     * Setter
     *
     * @param weight of type double
     */
    public void setWeight(double weight) {
        this.weight = weight;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int idx = in.readInt();
        EdgeType et = EdgeType.values()[idx];
        setType(et);
        setWeight(in.readDouble());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        EdgeType et = getType();
        out.writeInt(et.ordinal());
        out.writeDouble(getWeight());
    }

    /**
     * Read edge data from DataInput
     *
     * @param in of type DataInput
     * @return EdgeDataWritable
     * @throws IOException
     */
    public static EdgeDataWritable read(DataInput in) throws IOException {
        EdgeDataWritable writable = new EdgeDataWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write edge data to DataOutput
     *
     * @param out of type DataOutput
     * @param type of type EdgeType
     * @param weight of type double
     * @throws IOException
     */
    public static void write(DataOutput out, EdgeType type, double weight) throws IOException {
        new EdgeDataWritable(type, weight).write(out);
    }

}
