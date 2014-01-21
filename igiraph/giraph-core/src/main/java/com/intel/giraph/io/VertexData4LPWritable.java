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

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of VertexData4LP (label propagation)
 */
public final class VertexData4LPWritable implements Writable {
    /** prior vector of this vertex */
    private final VectorWritable priorWritable = new VectorWritable();
    /** posterior vector of this vertex */
    private final VectorWritable posteriorWritable = new VectorWritable();
    /** degree of this vertex */
    private double degree = 0;
    /**
     * Default constructor
     */
    public VertexData4LPWritable() {
    }

    /**
     * Constructor
     *
     * @param prior of type vector
     * @param posterior of type vector
     * @param degree of type double
     */
    public VertexData4LPWritable(Vector prior, Vector posterior, double degree) {
        this.priorWritable.set(prior);
        this.posteriorWritable.set(posterior);
        this.degree = degree;
    }

    /**
     * Getter
     *
     * @return prior vector
     */
    public Vector getPriorVector() {
        return priorWritable.get();
    }

    /**
     * Setter
     *
     * @param vector of type Vector
     */
    public void setPriorVector(Vector vector) {
        priorWritable.set(vector);
    }

    /**
     * Getter
     *
     * @return posterior vector
     */
    public Vector getPosteriorVector() {
        return posteriorWritable.get();
    }

    /**
     * Setter
     *
     * @param vector of type Vector
     */
    public void setPosteriorVector(Vector vector) {
        posteriorWritable.set(vector);
    }

    /**
     * Getter
     *
     * @return degree of type double
     */
    public double getDegree() {
        return degree;
    }

    /**
     * Setter
     *
     * @param degree of type double
     */
    public void setDegree(double degree) {
        this.degree = degree;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        priorWritable.readFields(in);
        posteriorWritable.readFields(in);
        degree = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        priorWritable.write(out);
        posteriorWritable.write(out);
        out.writeDouble(degree);
    }

    /**
     * Read vertex data to DataInput
     *
     * @param in of type DataInput
     * @return VertexData4LPWritable
     * @throws IOException
     */
    public static VertexData4LPWritable read(DataInput in) throws IOException {
        VertexData4LPWritable writable = new VertexData4LPWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write vertex data to DataOutput
     *
     * @param out of type DataOutput
     * @param ssv1 of type SequentialAccessSparseVector
     * @param ssv2 of type SequentialAccessSparseVector
     * @param degree of type double
     * @throws IOException
     */
    public static void write(DataOutput out, SequentialAccessSparseVector ssv1,
        SequentialAccessSparseVector ssv2, double degree) throws IOException {
        new VertexData4LPWritable(ssv1, ssv2, degree).write(out);
    }

}
