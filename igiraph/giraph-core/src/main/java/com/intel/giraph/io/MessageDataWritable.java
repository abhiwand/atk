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

package com.intel.giraph.io;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.SequentialAccessSparseVector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.intel.giraph.io.EdgeDataWritable.EdgeType;

/**
 * Writable to handle serialization of the fields associated with message data
 */
public class MessageDataWritable implements Writable {

    /** the vertex data for this message */
    private final VertexDataWritable vertexDataWritable = new VertexDataWritable();

    /** the edge data for this message */
    private final EdgeDataWritable edgeDataWritable = new EdgeDataWritable();

    /**
     * Default constructor
     */
    public MessageDataWritable() {
    }

    /**
     * Constructor
     *
     * @param vector of type Vector
     * @param bias of type double
     * @param type of type EdgeType
     * @param weight of type double
     */
    public MessageDataWritable(Vector vector, double bias, EdgeType type, double weight) {
        vertexDataWritable.setVector(vector);
        vertexDataWritable.setBias(bias);
        edgeDataWritable.setType(type);
        edgeDataWritable.setWeight(weight);
    }

    /**
     * Constructor
     *
     * @param vertex of type VertexDataWritable
     * @param edge of type EdgeDataWritable
     */
    public MessageDataWritable(VertexDataWritable vertex, EdgeDataWritable edge) {
        vertexDataWritable.setVector(vertex.getVector());
        vertexDataWritable.setBias(vertex.getBias());
        edgeDataWritable.setType(edge.getType());
        edgeDataWritable.setWeight(edge.getWeight());
    }

    /**
     * Setter
     *
     * @param weight of type double
     */
    public void setWeight(double weight) {
        edgeDataWritable.setWeight(weight);
    }

    /**
     * Getter
     *
     * @return weight of type double
     */
    public double getWeight() {
        return edgeDataWritable.getWeight();
    }

    /**
     * Setter
     *
     * @param type of type EdgeType
     */
    public void setType(EdgeType type) {
        edgeDataWritable.setType(type);
    }

    /**
     * Getter
     *
     * @return type of type EdgeType
     */
    public EdgeType getType() {
        return edgeDataWritable.getType();
    }

    /**
     * Getter
     *
     * @return vector of type Vector
     */
    public Vector getVector() {
        return vertexDataWritable.getVector();
    }

    /**
     * Setter
     *
     * @param vector of type Vector
     */
    public void setVector(Vector vector) {
        vertexDataWritable.setVector(vector);
    }

    /**
     * Setter
     *
     * @param bias of type double
     */
    public void setBias(double bias) {
        vertexDataWritable.setBias(bias);
    }

    /**
     * Getter
     *
     * @return bias of type double
     */
    public double getBias() {
        return vertexDataWritable.getBias();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        edgeDataWritable.readFields(in);
        vertexDataWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        edgeDataWritable.write(out);
        vertexDataWritable.write(out);
    }

    /**
     * Read message data from DataInput
     *
     * @param in of type DataInput
     * @return MessageDataWritable
     * @throws IOException
     */
    public static MessageDataWritable read(DataInput in) throws IOException {
        MessageDataWritable writable = new MessageDataWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write message data to DataOutput
     *
     * @param out of type DataOutput
     * @param type of type EdgeType
     * @param weight of type Double
     * @param ssv of type SequentailAccessSparseVector
     * @param bias of type double
     * @throws IOException
     */
    public static void write(DataOutput out, SequentialAccessSparseVector ssv, double bias,
        EdgeType type, double weight) throws IOException {
        new MessageDataWritable(ssv, bias, type, weight).write(out);
    }

}
