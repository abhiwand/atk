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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of a vector and an associated wordCount
 */
public final class LdaEdgeData implements Writable {

    private Double wordCount = null;
    private final VectorWritable vectorWritable = new VectorWritable(new DenseVector());

    public LdaEdgeData() {
    }

    /**
     * Constructor
     *
     * @param wordCount of type double
     */
    public LdaEdgeData(double wordCount) {
        this.wordCount = wordCount;
    }

    /**
     * @deprecated this Constructor is the one Titan used but I don't think we need it
     */
    public LdaEdgeData(double wordCount, Vector vector) {
        setWordCount(wordCount);
        setVector(vector);
    }

    public void setWordCount(Double data) {
        this.wordCount = data;
    }

    public Double getWordCount() {
        return wordCount;
    }

    public Vector getVector() {
        return vectorWritable.get();
    }

    public void setVector(Vector vector) {
        vectorWritable.set(vector);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        wordCount = in.readDouble();
        vectorWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(wordCount);
        vectorWritable.write(out);
    }

}
