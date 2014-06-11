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
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of the fields associated with VertexData4CF
 */
public class VertexData4CFWritable implements Writable {

    /** The vertex type supported by this vertex */
    public enum VertexType { LEFT, RIGHT };

    /** The type of this vertex */
    private VertexType type = null;

    /** The vector value at this vertex */
    private final VectorWritable vectorWritable = new VectorWritable();

    /** The bias value at this vertex */
    private double bias = 0d;

    /**
     * Default constructor
     */
    public VertexData4CFWritable() {
    }

    /**
     * Constructor
     *
     * @param type of type VertexType
     * @param vector of type Vector
     */
    public VertexData4CFWritable(VertexType type, Vector vector) {
        this.type = type;
        vectorWritable.set(vector);
    }

    /**
     * Constructor
     *
     * @param type of type VertexType
     * @param vector of type Vector
     * @param bias of type double
     */
    public VertexData4CFWritable(VertexType type, Vector vector, double bias) {
        this.type = type;
        vectorWritable.set(vector);
        this.bias = bias;
    }

    /**
     * Setter
     *
     * @param type of type VertexType
     */
    public void setType(VertexType type) {
        this.type = type;
    }

    /**
     * Getter
     *
     * @return VertexType
     */
    public VertexType getType() {
        return type;
    }

    /**
     * Getter
     *
     * @return Vector
     */
    public Vector getVector() {
        return vectorWritable.get();
    }

    /**
     * Setter
     *
     * @param vector of type Vector
     */
    public void setVector(Vector vector) {
        vectorWritable.set(vector);
    }

    /**
     * Getter
     *
     * @return bias of type double
     */
    public double getBias() {
        return bias;
    }

    /**
     * Setter
     *
     * @param bias of type double
     */
    public void setBias(double bias) {
        this.bias = bias;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int idx = in.readInt();
        VertexType vt = VertexType.values()[idx];
        setType(vt);
        vectorWritable.readFields(in);
        bias = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        VertexType vt = getType();
        out.writeInt(vt.ordinal());
        vectorWritable.write(out);
        out.writeDouble(bias);
    }

    /**
     * Read vertex data from DataInput
     *
     * @param in of type DataInput
     * @return VertexDataWritable
     * @throws IOException
     */
    public static VertexData4CFWritable read(DataInput in) throws IOException {
        VertexData4CFWritable writable = new VertexData4CFWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write vertex data to DataOutput
     *
     * @param out of type DataOutput
     * @param type of type VertexType
     * @param ssv of type SequentailAccessSparseVector
     * @throws IOException
     */
    public static void write(DataOutput out, VertexType type, SequentialAccessSparseVector ssv) throws IOException {
        new VertexData4CFWritable(type, ssv).write(out);
    }

    /**
     * Write vertex data to DataOutput
     *
     * @param out of type DataOutput
     * @param type of type VertexType
     * @param ssv of type SequentailAccessSparseVector
     * @param bias of type double
     * @throws IOException
     */
    public static void write(DataOutput out, VertexType type, SequentialAccessSparseVector ssv, double bias)
        throws IOException {
        new VertexData4CFWritable(type, ssv, bias).write(out);
    }

}
