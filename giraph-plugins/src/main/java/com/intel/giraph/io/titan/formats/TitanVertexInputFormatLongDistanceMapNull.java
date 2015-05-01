//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.giraph.io.titan.formats;

import com.intel.giraph.io.DistanceMapWritable;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

/**
 * TitanHBaseVertexInputFormatLongDistanceMapNull loads vertex
 * with <code>Long</code> vertex ID's,
 * <code>DistanceMap</code> vertex values,
 * and <code>Null</code> edge weights.
 */
public class TitanVertexInputFormatLongDistanceMapNull extends
        TitanVertexInputFormat<LongWritable, DistanceMapWritable, NullWritable> {

    /**
     * Constructs Giraph vertex reader
     * <p/>
     * Reads Giraph vertex from Titan/HBase table.
     *
     * @param split   Input split from HBase table
     * @param context Giraph task context
     * @throws IOException
     */
    @Override
    public VertexReader<LongWritable, DistanceMapWritable, NullWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new LongDistanceMapNullVertexReader(split, context);
    }

    /**
     * Uses the RecordReader to return HBase data
     */
    protected static class LongDistanceMapNullVertexReader extends TitanVertexReaderCommon<DistanceMapWritable, NullWritable> {

        /**
         * TitanHBaseVertexReader constructor
         *
         * @param split   InputSplit from TableInputFormat
         * @param context task context
         * @throws IOException
         */
        public LongDistanceMapNullVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            super(split, context);
        }

        /**
         * Construct a Giraph vertex from a Faunus (Titan/Hadoop) vertex.
         *
         * @param conf         Giraph configuration with property names, and edge labels to filter
         * @param faunusVertex Faunus vertex
         * @return Giraph vertex
         */
        @Override
        public Vertex<LongWritable, DistanceMapWritable, NullWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {

            // Initialize Giraph vertex
            Vertex<LongWritable, DistanceMapWritable, NullWritable> vertex = conf.createVertex();
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), new DistanceMapWritable());

            // Add egdes to Giraph vertex
            Iterator<TitanEdge> titanEdges = vertexBuilder.buildBlueprintsEdges(faunusVertex);
            addGiraphEdges(vertex, faunusVertex, titanEdges);

            return (vertex);
        }
    }
}
