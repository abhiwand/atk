/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.giraph.io.formats;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.Vector;
import com.intel.mahout.math.TwoVectorWritable;
import com.intel.mahout.math.DoubleWithVectorWritable;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>TwoVector</code> values and <code>DoubleWithVector</code>
 * out-edge weight/message
 */
public class JsonLongIDTwoVectorValueOutputFormat extends
  TextVertexOutputFormat<LongWritable, TwoVectorWritable,
  DoubleWithVectorWritable> {

  @Override
  public TextVertexWriter createVertexWriter(
      TaskAttemptContext context) {
    return new JsonLongIDTwoVectorValueWriter();
  }

 /**
  * VertexWriter that supports vertices with <code>TwoVector</code>
  * values and <code>DoubleWithVector</code> out-edge weight/message.
  */
  private class JsonLongIDTwoVectorValueWriter extends
    TextVertexWriterToEachLine {
    @Override
    public Text convertVertexToLine(
      Vertex<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> vertex)
      throws IOException {
      JSONArray jsonVertex = new JSONArray();
      try {
        jsonVertex.put(vertex.getId().get());
        JSONArray jsonValueArray = new JSONArray();
        Vector vector = vertex.getValue().getPosteriorVector();
        for (int i = 0; i < vector.size(); i++) {
          jsonValueArray.put(vector.getQuick(i));
        }
        jsonVertex.put(jsonValueArray);
      } catch (JSONException e) {
        throw new IllegalArgumentException(
          "writeVertex: Couldn't write vertex " + vertex);
      }
      return new Text(jsonVertex.toString());
    }
  }
}
