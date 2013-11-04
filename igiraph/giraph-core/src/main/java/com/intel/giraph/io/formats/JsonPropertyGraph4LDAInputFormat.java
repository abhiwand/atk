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

import com.google.common.collect.Lists;
import com.intel.giraph.io.VertexData4LDAWritable;
import com.intel.giraph.io.VertexData4LDAWritable.VertexType;
import com.intel.mahout.math.DoubleWithVectorWritable;

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
import org.apache.giraph.io.formats.TextVertexInputFormat;

import java.io.IOException;
import java.util.List;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>VertexData4LDA</code> vertex values, and <code>DoubleWithVector</code> edge
  * values, specified in JSON format.
  */
public class JsonPropertyGraph4LDAInputFormat extends TextVertexInputFormat<LongWritable,
    VertexData4LDAWritable, DoubleWithVectorWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
        return new JsonPropertyGraph4LDAReader();
    }

    /**
     * VertexReader that features <code>VertexData4LDA</code> vertex values and
     * <code>DoubleWithVector</code> out-edge info. The files should be in the following
     * JSON format:
     * JSONArray(<vertex id>, <vertex valueVector>, <vertex property>
     * JSONArray(JSONArray(<dest vertex id>, <edge value>, <edge property>), ...))
     * Here is an example with vertex id 1, vertex value 4,3 marked as "d",
     * and two edges. First edge has a destination vertex 2, edge value 2.1 with empty
     * edge property. Second edge has a destination vertex 3, edge value 0.7 with empty
     * edge property. [1,[4,3],[d],[[2,2.1,[]],[3,0.7,[]]]]
     */
    class JsonPropertyGraph4LDAReader extends
        TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {
        /** The length of vertex value vector */
        private int cardinality = -1;

        @Override
        protected JSONArray preprocessLine(Text line) throws JSONException {
            return new JSONArray(line.toString());
        }

        @Override
        protected LongWritable getId(JSONArray jsonVertex) throws JSONException, IOException {
            return new LongWritable(jsonVertex.getLong(0));
        }

        @Override
        protected VertexData4LDAWritable getValue(JSONArray jsonVertex) throws JSONException, IOException {
            Vector vector = getDenseVector(jsonVertex.getJSONArray(1));
            if (cardinality != vector.size()) {
                if (cardinality == -1) {
                    cardinality = vector.size();
                } else {
                    throw new IllegalArgumentException("Error in input data: different cardinality!");
                }
            }
            VertexType vt = getVertexType(jsonVertex.getJSONArray(2));
            return new VertexData4LDAWritable(vt, vector);
        }

        @Override
        protected Iterable<Edge<LongWritable, DoubleWithVectorWritable>> getEdges(JSONArray jsonVertex)
            throws JSONException, IOException {
            JSONArray jsonEdgeArray = jsonVertex.getJSONArray(3);
            List<Edge<LongWritable, DoubleWithVectorWritable>> edges =
                Lists.newArrayListWithCapacity(jsonEdgeArray.length());
            for (int i = 0; i < jsonEdgeArray.length(); ++i) {
                JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
                edges.add(EdgeFactory.create(new LongWritable(jsonEdge.getLong(0)),
                    new DoubleWithVectorWritable(jsonEdge.getDouble(1), new DenseVector())));
            }
            return edges;
        }

        @Override
        protected Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> handleException(Text line,
            JSONArray jsonVertex, JSONException e) {
            throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
        }

        /**
         * get DenseVector from JSONArray
         *
         * @param valueVector the JSONArray to use
         * @return denseVector the generated DenseVector
         * @throws JSONException
         */
        protected DenseVector getDenseVector(JSONArray valueVector) throws JSONException {
            double[] values = new double[valueVector.length()];
            for (int i = 0; i < valueVector.length(); i++) {
                values[i] = valueVector.getDouble(i);
            }
            return new DenseVector(values);
        }

        /**
         * get vertex type from JSONArray
         *
         * @param valueVector the JSONArray to use
         * @return VertexType
         * @throws JSONException
         */
        protected VertexType getVertexType(JSONArray valueVector) throws JSONException {
            if (valueVector.length() != 1) {
                throw new IllegalArgumentException("This vertex can only have one type.");
            }
            String vs = valueVector.getString(0);
            VertexType vt = null;
            if (vs.equals("d")) {
                vt = VertexType.DOC;
            } else if (vs.equals("w")) {
                vt = VertexType.WORD;
            } else {
                throw new IllegalArgumentException(String.format("Vertex type string: %s isn't supported.", vs));
            }
            return vt;
        }

    }

}
