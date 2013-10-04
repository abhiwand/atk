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
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of two vectors: prior and posterior
 */
public final class TwoVectorWritable implements Writable {
    /** prior vector */
    private final VectorWritable priorWritable = new VectorWritable();
    /** posterior vector */
    private final VectorWritable posteriorWritable = new VectorWritable();

    /**
     * Default constructor
     */
    public TwoVectorWritable() {
    }

    /**
     * Constructor
     *
     * @param prior of type vector
     * @param posterior of type vector
     */
    public TwoVectorWritable(Vector prior, Vector posterior) {
        this.priorWritable.set(prior);
        this.posteriorWritable.set(posterior);
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
     * @param vector for prior
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
     * @param vector for posterior
     */
    public void setPosteriorVector(Vector vector) {
        posteriorWritable.set(vector);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        priorWritable.readFields(in);
        posteriorWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        priorWritable.write(out);
        posteriorWritable.write(out);
    }

    /**
     * Read prior and posterior to DataInput
     *
     * @param in of type DataInput
     * @return TwoVectorWritable
     * @throws IOException
     */
    public static TwoVectorWritable read(DataInput in) throws IOException {
        TwoVectorWritable writable = new TwoVectorWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write prior and posterior to DataOutput
     *
     * @param out of type DataOutput
     * @param ssv1 of type SequentialAccessSparseVector
     * @param ssv2 of type SequentialAccessSparseVector
     * @throws IOException
     */
    public static void write(DataOutput out, SequentialAccessSparseVector ssv1,
        SequentialAccessSparseVector ssv2) throws IOException {
        new TwoVectorWritable(ssv1, ssv2).write(out);
    }

}
