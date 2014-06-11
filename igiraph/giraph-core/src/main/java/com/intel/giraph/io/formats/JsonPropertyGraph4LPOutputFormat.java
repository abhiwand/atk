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
package com.intel.giraph.io.formats;

import com.intel.giraph.io.VertexData4LPWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.Vector;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>Long</code> id and <code>VertexData4LP</code> values. Both prior
 * and posterior are output.
 */
public class JsonPropertyGraph4LPOutputFormat extends TextVertexOutputFormat<LongWritable,
    VertexData4LPWritable, Writable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new JsonPropertyGraph4LPWriter();
    }
    /**
     * VertexWriter that supports vertices with <code>Long</code> id
     * and <code>VertexData4LP</code> values.
     */
    protected class JsonPropertyGraph4LPWriter extends TextVertexWriterToEachLine {
        @Override
        public Text convertVertexToLine(Vertex<LongWritable, VertexData4LPWritable, Writable> vertex)
            throws IOException {
            JSONArray jsonVertex = new JSONArray();
            try {
                // Add id
                jsonVertex.put(vertex.getId().get());
                // Add prior
                JSONArray jsonPriorArray = new JSONArray();
                Vector prior = vertex.getValue().getPriorVector();
                for (int i = 0; i < prior.size(); i++) {
                    jsonPriorArray.put(prior.getQuick(i));
                }
                jsonVertex.put(jsonPriorArray);
                // Add posterior
                JSONArray jsonPosteriorArray = new JSONArray();
                Vector posterior = vertex.getValue().getPosteriorVector();
                for (int i = 0; i < posterior.size(); i++) {
                    jsonPosteriorArray.put(posterior.getQuick(i));
                }
                jsonVertex.put(jsonPosteriorArray);
            } catch (JSONException e) {
                throw new IllegalArgumentException("writeVertex: Couldn't write vertex " + vertex);
            }
            return new Text(jsonVertex.toString());
        }
    }
}
