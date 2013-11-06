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

package com.intel.giraph.io.formats;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.Vector;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;

import com.intel.giraph.io.VertexDataWritable;
import com.intel.giraph.io.VertexDataWritable.VertexType;

import java.io.IOException;

/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>Long</code> id and <code>VertexData</code> values.
 */
public class JsonPropertyGraph4CFOutputFormat extends TextVertexOutputFormat<LongWritable,
    VertexDataWritable, Writable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new JsonLongIDVertexDataWriter();
    }

    /**
     * VertexWriter that supports vertices with <code>Long</code> id
     * and <code>VertexData</code> values.
     */
    protected class JsonLongIDVertexDataWriter extends TextVertexWriterToEachLine {

        @Override
        public Text convertVertexToLine(Vertex<LongWritable, VertexDataWritable, Writable> vertex) throws IOException {
            JSONArray jsonVertex = new JSONArray();
            try {
                // add vertex id
                jsonVertex.put(vertex.getId().get());
                // add bias
                JSONArray jsonBiasArray = new JSONArray();
                jsonBiasArray.put(vertex.getValue().getBias());
                jsonVertex.put(jsonBiasArray);
                // add vertex value
                JSONArray jsonValueArray = new JSONArray();
                Vector vector = vertex.getValue().getVector();
                for (int i = 0; i < vector.size(); i++) {
                    jsonValueArray.put(vector.getQuick(i));
                }
                jsonVertex.put(jsonValueArray);
                // add vertex type
                JSONArray jsonTypeArray = new JSONArray();
                VertexType vt = vertex.getValue().getType();
                String vs = null;
                switch (vt) {
                case LEFT:
                    vs = "l";
                    break;
                case RIGHT:
                    vs = "r";
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Unrecognized vertex type: %s", vt.toString()));
                }
                jsonTypeArray.put(vs);
                jsonVertex.put(jsonTypeArray);
            } catch (JSONException e) {
                throw new IllegalArgumentException("writeVertex: Couldn't write vertex " + vertex);
            }
            return new Text(jsonVertex.toString().replaceAll("\"", ""));
        }
    }

}
