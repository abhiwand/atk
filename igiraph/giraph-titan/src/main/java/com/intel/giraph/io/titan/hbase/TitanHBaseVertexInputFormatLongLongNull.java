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

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
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
 * TitanHBaseVertexInputFormatLongLongNull loads vertex
 * with <code>Long</code> vertex ID's,
 * <code>Long</code> vertex values,
 * and <code>Null</code> edge weights.
 */
public class TitanHBaseVertexInputFormatLongLongNull extends
        TitanHBaseVertexInputFormat<LongWritable, LongWritable, NullWritable> {

    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexInputFormatLongLongNull.class);

    /**
     * Create vertex reader
     *
     * @param split   : inputsplits from TableInputFormat
     * @param context : task context
     * @return VertexReader
     * @throws IOException
     * @throws RuntimeException
     */
    @Override
    public VertexReader<LongWritable, LongWritable, NullWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new LongLongNullVertexReader(split, context);
    }

    /**
     * Vertex Reader that constructs Giraph vertices from Titan Vertices
     */
    protected static class LongLongNullVertexReader extends TitanHBaseVertexReader<LongWritable, LongWritable, NullWritable> {

        private Vertex<LongWritable, LongWritable, NullWritable> giraphVertex = null;

        /**
         * Constructs vertex reader
         *
         * @param split   Input split from HBase table
         * @param context Giraph task context
         * @throws IOException
         */
        public LongLongNullVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            this.context = context;
        }

        /**
         * check whether these is nextVertex available
         *
         * @return boolean
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
        public Vertex<LongWritable, LongWritable, NullWritable> getCurrentVertex() throws IOException,
                InterruptedException {
            return giraphVertex;
        }

        /**
         * Construct a Giraph vertex from a Faunus (Titan/Hadoop vertex).
         *
         * @param conf         Giraph configuration with property names, and edge labels to filter
         * @param faunusVertex Faunus vertex
         * @return Giraph vertex
         */
        private Vertex<LongWritable, LongWritable, NullWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {
            // Initialize Giraph vertex
            Vertex<LongWritable, LongWritable, NullWritable> vertex = conf.createVertex();
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), new LongWritable(0));

            // Add vertex properties
            Iterator<TitanProperty> titanProperties = vertexBuilder.buildTitanProperties(faunusVertex);
            vertex.setValue(getLongWritableProperty(titanProperties));

            // Add edges
            Iterator<TitanEdge> titanEdges = vertexBuilder.buildTitanEdges(faunusVertex);
            addGiraphEdges(vertex, faunusVertex, titanEdges);

            return (vertex);
        }


        /**
         * Create Long writable from value of first Titan property in iterator
         *
         * @param titanProperties Iterator of Titan properties
         * @return Long writable containing value of first property in list
         */
        private LongWritable getLongWritableProperty(Iterator<TitanProperty> titanProperties) {
            long vertexValue = 0;
            if (titanProperties.hasNext()) {
                Object propertyValue = titanProperties.next().getValue();
                try {
                    vertexValue = Long.parseLong(propertyValue.toString());
                } catch (NumberFormatException e) {
                    LOG.warn("Unable to parse long value for property: " + propertyValue);
                }
            }
            return (new LongWritable(vertexValue));
        }


        /**
         * Add edges to Giraph vertex.
         *
         * @param vertex       Giraph vertex to add edges to
         * @param faunusVertex (Faunus (Titan/Hadoop) vertex
         * @param titanEdges   Iterator of Titan edges
         */
        private void addGiraphEdges(Vertex<LongWritable, LongWritable, NullWritable> vertex,
                                    FaunusVertex faunusVertex, Iterator<TitanEdge> titanEdges) {
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
        private Edge<LongWritable, NullWritable> getGiraphEdge(FaunusVertex faunusVertex, TitanEdge titanEdge) {
            return EdgeFactory.create(new LongWritable(
                    titanEdge.getOtherVertex(faunusVertex).getLongId()), NullWritable.get());
        }
    }
}
