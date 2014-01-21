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
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
/**
 * Writable to handle serialization of the fields associated with vertex data
 */
public class VertexData4LBPWritable implements Writable {
    /** the vertex type supported by this vertex */
    public enum VertexType { TRAIN, VALIDATE, TEST };
    /** the type of this vertex */
    private VertexType type = null;
    /** prior vector */
    private final VectorWritable priorWritable = new VectorWritable();
    /** posterior vector */
    private final VectorWritable posteriorWritable = new VectorWritable();

    /**
     * Default constructor
     */
    public VertexData4LBPWritable() {
    }

    /**
     * Constructor
     *
     * @param type of type VertexType
     * @param prior of type Vector
     * @param posterior of type Vector
     */
    public VertexData4LBPWritable(VertexType type, Vector prior, Vector posterior) {
        this.type = type;
        priorWritable.set(prior);
        posteriorWritable.set(posterior);
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
     * @return type of type VertexType
     */
    public VertexType getType() {
        return type;
    }

    /**
     * Getter
     *
     * @return vector of type Vector
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
     * @return vector of type Vector
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

    @Override
    public void readFields(DataInput in) throws IOException {
        int idx = in.readInt();
        VertexType vt = VertexType.values()[idx];
        setType(vt);
        priorWritable.readFields(in);
        posteriorWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        VertexType vt = getType();
        out.writeInt(vt.ordinal());
        priorWritable.write(out);
        posteriorWritable.write(out);
    }

    /**
     * Read vertex data from DataInput
     *
     * @param in of type DataInput
     * @return VertexDataWritable
     * @throws IOException
     */
    public static VertexData4LBPWritable read(DataInput in) throws IOException {
        VertexData4LBPWritable writable = new VertexData4LBPWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write vertex data to DataOutput
     *
     * @param out of type DataOutput
     * @param type of type VertexType
     * @param ssPrior of type SequentailAccessSparseVector
     * @param ssPosterior of type SequentailAccessSparseVector
     * @throws IOException
     */
    public static void write(DataOutput out, VertexType type, SequentialAccessSparseVector ssPrior,
        SequentialAccessSparseVector ssPosterior) throws IOException {
        new VertexData4LBPWritable(type, ssPrior, ssPosterior).write(out);
    }

}
