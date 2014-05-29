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

import com.intel.giraph.io.GaussianDistWritable;
import com.intel.giraph.io.VertexData4GBPWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>Long</code> id and <code>VertexData4GBP</code> values.
 * Posterior mean is the output of linear solver.
 */
public class JsonPropertyGraph4GBPOutputFormat extends TextVertexOutputFormat<LongWritable,
    VertexData4GBPWritable, Writable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new JsonPropertyGraph4GBPWriter();
    }

    /**
     * VertexWriter that supports vertices with <code>Long</code> id
     * and <code>VertexData4GBP</code> values.
     */
    protected class JsonPropertyGraph4GBPWriter extends TextVertexWriterToEachLine {
        @Override
        public Text convertVertexToLine(Vertex<LongWritable, VertexData4GBPWritable, Writable> vertex)
            throws IOException {
            JSONArray jsonVertex = new JSONArray();
            try {
                // add id
                jsonVertex.put(vertex.getId().get());
                // add posterior mean
                GaussianDistWritable posterior = vertex.getValue().getPosterior();
                jsonVertex.put(posterior.getMean());
            } catch (JSONException e) {
                throw new IllegalArgumentException("writeVertex: Couldn't write vertex " + vertex);
            }
            return new Text(jsonVertex.toString());
        }
    }
}
