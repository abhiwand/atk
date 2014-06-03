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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import com.intel.giraph.io.VertexData4LBPWritable;
import com.intel.giraph.io.VertexData4LBPWritable.VertexType;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>VertexData4LBP</code> vertex values, and <code>Double</code>
  * edge weights, specified in JSON format.
  */
public class JsonPropertyGraph4LBP4M2InputFormat extends TextVertexInputFormat<LongWritable,
    VertexData4LBPWritable, DoubleWritable> {
    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
        return new JsonPropertyGraph4LBP4M2Reader();
    }

    /**
     * VertexReader that features <code>VertexData4LBP</code> vertex
     * values and <code>double</code> out-edge weights. The
     * files should be in the following JSON format:
     * JSONArray(<vertex id>, <vertex valueVector>, <vertex type>
     * JSONArray(JSONArray(<dest vertex id>, <edge value>, <edge type>), ...))
     * Here is an example with vertex id 1, vertex value 4,3, and two edges.
     * First edge has a destination vertex 2, edge value 2.1.
     * Second edge has a destination vertex 3, edge value 0.7.
     * [1,[4,3],["TR"],[[2,2.1,[]],[3,0.7,[]]]], where the empty edge-type
     * '[]' is reserved for future usage.
     */
    class JsonPropertyGraph4LBP4M2Reader extends
        TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {
        /** The length of vertex value vector */
        private int cardinality = 0;
        /** Data vector */
        private Vector vector = null;

        @Override
        protected JSONArray preprocessLine(Text line) throws JSONException {
            return new JSONArray(line.toString());
        }

        @Override
        protected LongWritable getId(JSONArray jsonVertex) throws JSONException, IOException {
            return new LongWritable(jsonVertex.getLong(0));
        }

        @Override
        protected VertexData4LBPWritable getValue(JSONArray jsonVertex) throws JSONException, IOException {
            Vector priorVector = getDenseVector(jsonVertex.getJSONArray(1));
            if (cardinality != priorVector.size()) {
                if (cardinality == 0) {
                    cardinality = priorVector.size();
                    double[] data = new double[cardinality];
                    vector = new DenseVector(data);
                } else {
                    throw new IllegalArgumentException("Error in input data: different cardinality!");
                }
            }
            VertexType vt = getVertexType(jsonVertex.getJSONArray(2));
            return new VertexData4LBPWritable(vt, priorVector, vector.clone());
        }

        @Override
        protected Iterable<Edge<LongWritable, DoubleWritable>> getEdges(JSONArray jsonVertex)
            throws JSONException, IOException {
            JSONArray jsonEdgeArray = jsonVertex.getJSONArray(3);
            List<Edge<LongWritable, DoubleWritable>> edges =
                Lists.newArrayListWithCapacity(jsonEdgeArray.length());
            for (int i = 0; i < jsonEdgeArray.length(); ++i) {
                edges.add(EdgeFactory.create(new LongWritable(jsonEdgeArray.getLong(i)),
                    new DoubleWritable(1d)));
            }
            return edges;
        }

        @Override
        protected Vertex<LongWritable, VertexData4LBPWritable, DoubleWritable> handleException(Text line,
            JSONArray jsonVertex, JSONException e) {
            throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
        }

        /**
         * Get DenseVector from JSONArray
         *
         * @param valueVector of type JSONArray
         * @return DenseVector
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
         * Get vertex type from JSONArray
         *
         * @param valueVector of type JSONArray
         * @return VertexType
         * @throws JSONException
         */
        protected VertexType getVertexType(JSONArray valueVector) throws JSONException {
            if (valueVector.length() != 1) {
                throw new IllegalArgumentException("The vertex can only have one type.");
            }
            String vs = valueVector.getString(0).toLowerCase();
            VertexType vt = null;
            if (vs.equals("tr")) {
                vt = VertexType.TRAIN;
            } else if (vs.equals("va")) {
                vt = VertexType.VALIDATE;
            } else if (vs.equals("te")) {
                vt = VertexType.TEST;
            } else {
                throw new IllegalArgumentException(String.format("Vertex type string: %s isn't supported.", vs));
            }
            return vt;
        }
    }

}
