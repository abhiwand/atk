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
