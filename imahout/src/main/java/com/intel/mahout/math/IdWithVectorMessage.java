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

package com.intel.mahout.math;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of a vector and an associated Id
 */
public final class IdWithVectorMessage extends NumberWithVectorWritable<Long> {

    /**
     * Default constructor
     */
    public IdWithVectorMessage() {
        super();
    }

    /**
     * Constructor
     *
     * @param id of type long
     * @param vector of type Vector
     */
    public IdWithVectorMessage(long id, Vector vector) {
        super(id, vector);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setData(in.readLong());
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(getData());
        super.write(out);
    }

}
