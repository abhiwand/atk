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

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;
import com.intel.mahout.math.TwoVectorWritable;
import com.intel.mahout.math.DoubleWithTwoVectorWritable;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import java.io.IOException;
import java.util.List;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>TwoVector</code> vertex values: one for prior and
  * one for posterior, and <code>DoubleWithTwoVector</code> edge
  * weights and bi-directional edge messages, specified in
  * JSON format.
  */
public class JsonLongTwoVectorDoubleTwoVectorInputFormat extends
  TextVertexInputFormat<LongWritable, TwoVectorWritable,
  DoubleWithTwoVectorWritable> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new JsonLongTwoVectorDoubleTwoVectorReader();
  }

 /**
  * VertexReader that features <code>double vector</code> vertex
  * values and <code>double</code> out-edge weights. The
  * files should be in the following JSON format:
  * JSONArray(<vertex id>, <vertex valueVector>,
  * JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
  * Here is an example with vertex id 1, vertex value 4,3, and two edges.
  * First edge has a destination vertex 2, edge value 2.1.
  * Second edge has a destination vertex 3, edge value 0.7.
  * [1,[4,3],[[2,2.1],[3,0.7]]]
  */
  class JsonLongTwoVectorDoubleTwoVectorReader extends
    TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
    JSONException> {
    /** The length of vertex value vector */
    private int cardinality = 0;
    /** Data vector */
    private Vector vector = null;

    @Override
    protected JSONArray preprocessLine(Text line) throws JSONException {
      return new JSONArray(line.toString());
    }

    @Override
    protected LongWritable getId(JSONArray jsonVertex) throws JSONException,
    IOException {
      return new LongWritable(jsonVertex.getLong(0));
    }

    @Override
    protected TwoVectorWritable getValue(JSONArray jsonVertex) throws
      JSONException, IOException {
      Vector priorVector = getDenseVector(jsonVertex.getJSONArray(1));
      if (cardinality != priorVector.size()) {
        if (cardinality == 0) {
          cardinality = priorVector.size();
          double[] data = new double[cardinality];
          vector = new DenseVector(data);
        } else {
          throw new IllegalArgumentException("Error in input data:" +
            "different cardinality!");
        }
      }
      return new TwoVectorWritable(priorVector, vector.clone());
    }

    @Override
    protected Iterable<Edge<LongWritable, DoubleWithTwoVectorWritable>>
    getEdges(JSONArray jsonVertex)
      throws JSONException, IOException {
      JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
      List<Edge<LongWritable, DoubleWithTwoVectorWritable>> edges =
          Lists.newArrayListWithCapacity(jsonEdgeArray.length());
      for (int i = 0; i < jsonEdgeArray.length(); ++i) {
        JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
        edges.add(EdgeFactory.create(new LongWritable(jsonEdge.getLong(0)),
          new DoubleWithTwoVectorWritable(jsonEdge.getDouble(1), vector.clone(),
              vector.clone())));
      }
      return edges;
    }

    @Override
    protected Vertex<LongWritable, TwoVectorWritable,
    DoubleWithTwoVectorWritable>
    handleException(Text line, JSONArray jsonVertex, JSONException e) {
      throw new IllegalArgumentException(
          "Couldn't get vertex from line " + line, e);
    }

    /**
     * get DenseVector from JSONArray
     *
     * @param valueVector the JSONArray to use
     * @return denseVector the generated DenseVector
     * @throws JSONException
     */
    protected DenseVector getDenseVector(JSONArray valueVector)
      throws JSONException {
      double[] values = new double[valueVector.length()];
      for (int i = 0; i < valueVector.length(); i++) {
        values[i] = valueVector.getDouble(i);
      }
      return new DenseVector(values);
    }
  }

}
