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

import com.intel.mahout.math.IdWithVectorWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.Vector;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>Long</code> id and <code>VectorWritable</code> values.
 */
public class JsonKMeansPPLongVectorOutputFormat extends TextVertexOutputFormat<LongWritable,
    IdWithVectorWritable, NullWritable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new JsonKMeansPPLongVectorWriter();
    }

    /**
     * VertexWriter that supports vertices with <code>Long</code> id
     * and <code>VectorWritable</code> values.
     */
    protected class JsonKMeansPPLongVectorWriter extends TextVertexWriterToEachLine {

        @Override
        public Text convertVertexToLine(Vertex<LongWritable, IdWithVectorWritable, NullWritable> vertex) throws 
            IOException {
            JSONArray jsonVertex = new JSONArray();
            try {
                long vertex_value_id = vertex.getValue().getData();
                System.out.println("vid=" + vertex.getId().get() + ", vertex_value_id=" + vertex_value_id);
                if (vertex_value_id >= 0) {
                    return null;
                }
                jsonVertex.put(vertex.getId().get());
                JSONArray jsonValueArray = new JSONArray();
                Vector vector = vertex.getValue().getVector();
                for (int i = 0; i < vector.size(); i++) {
                    jsonValueArray.put(vector.getQuick(i));
                }
                jsonVertex.put(jsonValueArray);
            } catch (JSONException e) {
                throw new IllegalArgumentException("writeVertex: Couldn't write vertex " + vertex);
            }
            return new Text(jsonVertex.toString());
        }
    }
}
