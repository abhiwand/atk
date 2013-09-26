//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2012 Intel Corporation All Rights Reserved.
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

package com.intel.mahout.math;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of a vector and an associated number
 *
 * @param <T> a subclass of Number
 */
public abstract class NumberWithVectorWritable<T extends Number> implements Writable {
    /** Data of type T */
    private T data = null;
    /** Data of type Vector */
    private final VectorWritable vectorWritable = new VectorWritable();

    /**
     * Default constructor
     */
    public NumberWithVectorWritable() {
    }

    /**
     * Constructor
     *
     * @param data of type T
     * @param vector of type Vector
     */
    public NumberWithVectorWritable(T data, Vector vector) {
        this.data = data;
        this.vectorWritable.set(vector);
    }

    /**
     * Setter
     *
     * @param data of type T
     */
    public void setData(T data) {
        this.data = data;
    }

    /**
     * Getter
     *
     * @return data of type double
     */
    public T getData() {
        return data;
    }

    /**
     * Getter
     *
     * @return vector of type Vector
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

    @Override
    public void readFields(DataInput in) throws IOException {
        vectorWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        vectorWritable.write(out);
    }

}
