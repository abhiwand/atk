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
package com.intel.giraph.io.titan.hbase;

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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * TitanHBaseVertexInputFormatLongDistanceMapNull loads vertex
 * with <code>Long</code> vertex ID's,
 * <code>DistanceMap</code> vertex values,
 * and <code>Null</code> edge weights.
 */
public class TitanHBaseVertexInputFormatLongDistanceMapNull extends
        TitanHBaseVertexInputFormat<LongWritable, DistanceMapWritable, NullWritable> {

    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexInputFormatLongDistanceMapNull.class);

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
    public static class LongDistanceMapNullVertexReader extends TitanHBaseVertexReader<LongWritable, DistanceMapWritable, NullWritable> {

        private Vertex<LongWritable, DistanceMapWritable, NullWritable> giraphVertex = null;

        /**
         * TitanHBaseVertexReader constructor
         *
         * @param split   InputSplit from TableInputFormat
         * @param context task context
         * @throws IOException
         */
        public LongDistanceMapNullVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            this.context = context;
        }

        /**
         * Gets the next Giraph vertex from the input split.
         *
         * @return boolean Returns True, if more vertices available
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            boolean hasMoreVertices = false;
            giraphVertex = null;

            if (getRecordReader().nextKeyValue()) {
                giraphVertex = readGiraphVertex(getConf(), getRecordReader().getCurrentValue());
                hasMoreVertices = true;
            }

            return hasMoreVertices;
        }

        /**
         * Get current Giraph vertex.
         *
         * @return Giraph vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Vertex<LongWritable, DistanceMapWritable, NullWritable> getCurrentVertex() throws IOException,
                InterruptedException {
            return giraphVertex;
        }

        /**
         * Construct a Giraph vertex from a Faunus (Titan/Hadoop) vertex.
         *
         * @param conf         Giraph configuration with property names, and edge labels to filter
         * @param faunusVertex Faunus vertex
         * @return Giraph vertex
         */
        private Vertex<LongWritable, DistanceMapWritable, NullWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {
            // Initialize Giraph vertex
            Vertex<LongWritable, DistanceMapWritable, NullWritable> vertex = conf.createVertex();
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), new DistanceMapWritable());

            // Add egdes to Giraph vertex
            Iterator<TitanEdge> titanEdges = vertexBuilder.buildTitanEdges(faunusVertex);
            addGiraphEdges(vertex, faunusVertex, titanEdges);

            return (vertex);
        }

        /**
         * Add edges to Giraph vertex.
         *
         * @param vertex       Giraph vertex to add edges to
         * @param faunusVertex (Faunus (Titan/Hadoop) vertex
         * @param titanEdges   Iterator of Titan edges
         */
        private void addGiraphEdges(Vertex<LongWritable, DistanceMapWritable, NullWritable> vertex,
                                    FaunusVertex faunusVertex,
                                    Iterator<TitanEdge> titanEdges) {
            while (titanEdges.hasNext()) {
                TitanEdge titanEdge = titanEdges.next();
                Edge<LongWritable, NullWritable> edge = getGiraphEdge(faunusVertex, titanEdge);
                vertex.addEdge(edge);
            }
        }

        /**
         * Create Giraph edge from Titan edge
         *
         * @param faunusVertex Faunus (Titan/Hadoop) vertex
         * @param titanEdge    Titan edge
         * @return Giraph edge
         */
        private Edge<LongWritable, NullWritable> getGiraphEdge(
                FaunusVertex faunusVertex, TitanEdge titanEdge) {
            return EdgeFactory.create(new LongWritable(
                    titanEdge.getOtherVertex(faunusVertex).getLongId()), NullWritable.get());
        }
    }
}
