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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.intel.giraph.io.EdgeData4CFWritable.EdgeType;
import com.intel.giraph.io.VertexData4CFWritable.VertexType;

/**
 * Writable to handle serialization of the fields associated with MessageData4CF
 */
public class MessageData4CFWritable implements Writable {

    /** The vertex data for this message */
    private final VertexData4CFWritable vertexDataWritable = new VertexData4CFWritable();

    /** The edge data for this message */
    private final EdgeData4CFWritable edgeDataWritable = new EdgeData4CFWritable();

    /**
     * Default constructor
     */
    public MessageData4CFWritable() {
    }

    /**
     * Constructor
     *
     * @param vector of type Vector
     * @param bias of type double
     * @param type of type EdgeType
     * @param weight of type double
     */
    public MessageData4CFWritable(Vector vector, double bias, EdgeType type, double weight) {
        // vertex type isn't used in message; here uses LEFT as default
        vertexDataWritable.setType(VertexType.LEFT);
        vertexDataWritable.setVector(vector);
        vertexDataWritable.setBias(bias);
        edgeDataWritable.setType(type);
        edgeDataWritable.setWeight(weight);
    }

    /**
     * Constructor
     *
     * @param vertex of type VertexData4CFWritable
     * @param edge of type EdgeData4CFWritable
     */
    public MessageData4CFWritable(VertexData4CFWritable vertex, EdgeData4CFWritable edge) {
        vertexDataWritable.setType(vertex.getType());
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
     * @return Vector
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
    public static MessageData4CFWritable read(DataInput in) throws IOException {
        MessageData4CFWritable writable = new MessageData4CFWritable();
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
        new MessageData4CFWritable(ssv, bias, type, weight).write(out);
    }

}
