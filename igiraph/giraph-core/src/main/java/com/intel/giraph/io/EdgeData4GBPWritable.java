//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of the fields associated with EdgeData4CF
 */
public class EdgeData4GBPWritable implements Writable {

    /** The weight value at this edge */
    private double weight = 0d;

    /** The weight value on the dst->src edge */
    private double reverseWeight = 0d;

    /**
     * Default constructor
     */
    public EdgeData4GBPWritable() {
    }

    /**
     * Constructor
     *
     * @param weight of type double
     * @param reverseWeight of type double
     */
    public EdgeData4GBPWritable(double weight, double reverseWeight) {
        this.weight = weight;
        this.reverseWeight = reverseWeight;
    }

    /**
     * Setter
     *
     * @param reverseWeight of type double
     */
    public void setReverseWeight(double reverseWeight) {
        this.reverseWeight = reverseWeight;
    }

    /**
     * Getter
     *
     * @return reverseWeight
     */
    public double getReverseWeight() {
        return reverseWeight;
    }

    /**
     * Setter
     *
     * @param weight of type double
     */
    public void setWeight(double weight) {
        this.weight = weight;
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
     * Get reverse edge data
     * @return reverseEdgeData of type EdgeData4GBPWritalbe
     */
    public EdgeData4GBPWritable getReverseEdgeData() {
        EdgeData4GBPWritable reverseEdgeData = new EdgeData4GBPWritable();
        reverseEdgeData.setWeight(reverseWeight);
        reverseEdgeData.setReverseWeight(weight);
        return reverseEdgeData;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setWeight(in.readDouble());
        setReverseWeight(in.readDouble());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(getWeight());
        out.writeDouble(getReverseWeight());
    }

    /**
     * Read edge data from DataInput
     *
     * @param in of type DataInput
     * @return EdgeDataWritable
     * @throws IOException
     */
    public static EdgeData4GBPWritable read(DataInput in) throws IOException {
        EdgeData4GBPWritable writable = new EdgeData4GBPWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write edge data to DataOutput
     *
     * @param out of type DataOutput
     * @param weight of type double
     * @param reverseWeight of type double
     * @throws IOException
     */
    public static void write(DataOutput out, double weight, double reverseWeight) throws IOException {
        new EdgeData4GBPWritable(weight, reverseWeight).write(out);
    }

}
