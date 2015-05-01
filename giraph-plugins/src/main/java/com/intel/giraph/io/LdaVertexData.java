//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.spark.mllib.ia.plugins.VectorUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of the fields associated with vertex data of LDA
 */
public class LdaVertexData implements Writable {

    /** The vector value at this vertex */
    private final VectorWritable ldaResult = new VectorWritable(new DenseVector());

    public LdaVertexData() {
    }

    public void setLdaResult(Vector vector) {
        ldaResult.set(vector);
    }

    public Vector getLdaResult() {
        return ldaResult.get();
    }

    public double[] getLdaResultAsDoubleArray() {
        return VectorUtils.toDoubleArray(getLdaResult());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ldaResult.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ldaResult.write(out);
    }
}
