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
import org.apache.giraph.edge.Edge;
//import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
//import com.intel.mahout.math.DoubleWithVectorWritable;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import java.io.IOException;
import java.util.List;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>double Vector</code> vertex values specified in JSON format.
  */
public class JsonLongIDDoubleVectorInputFormat extends
    TextVertexInputFormat<LongWritable, VectorWritable, NullWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
        return new JsonLongIDDoubleVectorReader();
    }

    /**
     * VertexReader that features <code>Vector</code> vertex valuess.
     * Input files should be in the following JSON format:
     * JSONArray(<vertex id>, JSONArray(<vertex valueVector>)).
     *
     * Example: [1, [1.2, 0.5, -0.01, 3.5, 5.1]]
     * \li Vertex id: 1
     * \li Vertex values: 1.2, 0.5, -0.01, 3.5, and 5.1.
     *
     */
    class JsonLongIDDoubleVectorReader extends
        TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {

        @Override
        protected JSONArray preprocessLine(Text line) throws JSONException {
            return new JSONArray(line.toString());
        }

        @Override
        protected LongWritable getId(JSONArray jsonVertex) throws JSONException, IOException {
            return new LongWritable(jsonVertex.getLong(0));
        }

        @Override
        protected VectorWritable getValue(JSONArray jsonVertex) throws JSONException, IOException {
            Vector vec = getDenseVector(jsonVertex.getJSONArray(1));
            return new VectorWritable(vec);
        }

        @Override
        protected Iterable<Edge<LongWritable, NullWritable>> getEdges(JSONArray jsonVertex)
            throws JSONException, IOException {
            List<Edge<LongWritable, NullWritable>> edges = Lists.newArrayListWithCapacity(0);
            return edges;
        }

        @Override
        protected Vertex<LongWritable, VectorWritable, NullWritable> handleException(Text line,
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
    }
}
