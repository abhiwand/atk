/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.giraph.io.formats;

import com.google.common.collect.Lists;
import com.intel.giraph.io.GaussianDistWritable;
import com.intel.giraph.io.VertexData4GBPWritable;
import org.apache.giraph.edge.Edge;
import com.intel.giraph.io.EdgeData4GBPWritable;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;
import java.util.List;

/**
 * VertexInputFormat that features <code>long</code> vertex ID's,
 * <code>VertexData4GBP</code> vertex values, and <code>Double</code>
 * edge weights, specified in JSON format.
 */
public class JsonPropertyGraph4GBPInputFormat extends TextVertexInputFormat<LongWritable,
    VertexData4GBPWritable, EdgeData4GBPWritable> {
    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
        return new JsonPropertyGraph4GBPReader();
    }

    /**
     * VertexReader that features <code>VertexData4GBP</code> vertex
     * values and <code>double</code> out-edge weights. The
     * files should be in the following JSON format:
     * JSONArray(<vertex id>, <mean, precision>
     * JSONArray(JSONArray(<dest vertex id>, <edge weight>, <reverse edge's weight></>), ...))
     * Here is an example with vertex id 1, vertex value 4,3, and two edges.
     * First edge has a destination vertex 2, edge value 2.1;
     * and the edge value from vertex 2 to vertex 1 is 1.3.
     * Second edge has a destination vertex 3, edge value 0.7;
     * and the edge value from vertex 3 to vertex 1 is 2.5.
     * [1,[4,3],[[2,2.1,1.3],[3,0.7,2.5]]].
     * If the graph is symmetric, then only to specify edge weight.
     * Same weight will be used for the reverse edge.
     * For example, [1,[4,3],[[2,3.0],[3,5.0]]] means the weight on edge
     * from vertex 1 to vertex 2 is 3.0, and the weight on edge from
     * vertex 2 to vertex 1 is also 3.0.
     */
    class JsonPropertyGraph4GBPReader extends
        TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {
        /** index for vertex id */
        private static final int VERTEX_ID_IDX = 0;
        /** index for vertex value vector */
        private static final int VERTEX_VALUE_IDX = 1;
        /** index for edge array */
        private static final int EDGE_ARRAY_IDX = 2;
        /** index for mean */
        private static final int MEAN_IDX = 0;
        /** index for precision */
        private static final int PRECISION_IDX = 1;
        /** length of vertex value vector */
        private static final int VERTEX_VALUE_LENGTH = 2;
        /** index for target id */
        private static final int TARGET_ID_IDX = 0;
        /** index for weight */
        private static final int WEIGHT_IDX = 1;
        /** index for reverse weight */
        private static final int REVERSE_WEIGHT_IDX = 2;
        /** minimal length of edge value vector */
        private static final int MIN_EDGE_VALUE_LENGTH = 2;
        /** maximum length of edge value vector */
        private static final int MAX_EDGE_VALUE_LENGTH = 3;
        /** default mean */
        private static final double DEFAULT_MEAN = 0d;

        @Override
        protected JSONArray preprocessLine(Text line) throws JSONException {
            return new JSONArray(line.toString());
        }

        @Override
        protected LongWritable getId(JSONArray jsonVertex) throws JSONException, IOException {
            return new LongWritable(jsonVertex.getLong(VERTEX_ID_IDX));
        }

        @Override
        protected VertexData4GBPWritable getValue(JSONArray jsonVertex) throws JSONException, IOException {
            JSONArray vector = jsonVertex.getJSONArray(VERTEX_VALUE_IDX);
            if (vector.length() != VERTEX_VALUE_LENGTH) {
                throw new IllegalArgumentException("Error in vertex data: mean and precision are needed!");
            }
            GaussianDistWritable prior = new GaussianDistWritable();
            GaussianDistWritable posterior = new GaussianDistWritable();
            prior.setMean(vector.getDouble(MEAN_IDX));
            prior.setPrecision(vector.getDouble(PRECISION_IDX));
            return new VertexData4GBPWritable(prior, posterior, prior, DEFAULT_MEAN);
        }

        @Override
        protected Iterable<Edge<LongWritable, EdgeData4GBPWritable>> getEdges(JSONArray jsonVertex)
            throws JSONException, IOException {
            JSONArray jsonEdgeArray = jsonVertex.getJSONArray(EDGE_ARRAY_IDX);
            List<Edge<LongWritable, EdgeData4GBPWritable>> edges =
                Lists.newArrayListWithCapacity(jsonEdgeArray.length());
            for (int i = 0; i < jsonEdgeArray.length(); ++i) {
                JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
                double reverseWeight;
                if (jsonEdge.length() == MAX_EDGE_VALUE_LENGTH) {
                    reverseWeight = jsonEdge.getDouble(REVERSE_WEIGHT_IDX);
                } else if (jsonEdge.length() == MIN_EDGE_VALUE_LENGTH) {
                    reverseWeight = jsonEdge.getDouble(WEIGHT_IDX);
                } else {
                    throw new IllegalArgumentException("at least target ID and edge value are needed.");
                }
                edges.add(EdgeFactory.create(new LongWritable(jsonEdge.getLong(TARGET_ID_IDX)),
                    new EdgeData4GBPWritable(jsonEdge.getDouble(WEIGHT_IDX), reverseWeight)));
            }
            return edges;
        }

        @Override
        protected Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> handleException(Text line,
            JSONArray jsonVertex, JSONException e) {
            throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
        }

    }

}
