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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * TitanHBaseVertexInputFormatLongDoubleNull loads vertex
 * with <code>Long</code> vertex ID's,
 * <code>Double</code> vertex values,
 * and <code>Float</code> edge weights.
 */
public class TitanHBaseVertexInputFormatLongDoubleNull extends
        TitanHBaseVertexInputFormat<LongWritable, DoubleWritable, NullWritable> {

    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexInputFormatLongDoubleNull.class);

    /**
     * Create vertex reader for Titan vertices
     *
     * @param split   Input splits for HBase table
     * @param context Giraph task context
     * @return VertexReader Vertex reader for Giraph vertices
     * @throws IOException
     * @throws RuntimeException
     */
    @Override
    public VertexReader<LongWritable, DoubleWritable, NullWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new LongDoubleNullVertexReader(split, context);
    }

    /**
     * Uses the RecordReader to return HBase data
     */
    public static class LongDoubleNullVertexReader extends TitanHBaseVertexReader<LongWritable, DoubleWritable, NullWritable> {

        private Vertex<LongWritable, DoubleWritable, NullWritable> giraphVertex = null;

        /**
         * Constructs Giraph vertex reader
         * <p/>
         * Reads Giraph vertex from Titan/HBase table.
         *
         * @param split   Input split from HBase table
         * @param context Giraph task context
         * @throws IOException
         */
        public LongDoubleNullVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
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
        public Vertex<LongWritable, DoubleWritable, NullWritable> getCurrentVertex() throws IOException,
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
        private Vertex<LongWritable, DoubleWritable, NullWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {

            // Initialize Giraph vertex
            Vertex<LongWritable, DoubleWritable, NullWritable> vertex = conf.createVertex();
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), new DoubleWritable(0));

            // Add vertex properties
            Iterator<TitanProperty> titanProperties = vertexBuilder.buildTitanProperties(faunusVertex);
            vertex.setValue(getDoubleWritableProperty(titanProperties));

            // Add edges
            Iterator<TitanEdge> titanEdges = vertexBuilder.buildTitanEdges(faunusVertex);
            addGiraphEdges(vertex, faunusVertex, titanEdges);

            return (vertex);
        }

        /**
         * Create Double writable from value of first Titan property in iterator
         *
         * @param titanProperties Iterator of Titan properties
         * @return Double writable containing value of first property in list
         */
        private DoubleWritable getDoubleWritableProperty(Iterator<TitanProperty> titanProperties) {
            double vertexValue = 0;
            if (titanProperties.hasNext()) {
                Object propertyValue = titanProperties.next().getValue();
                try {
                    vertexValue = Double.parseDouble(propertyValue.toString());
                } catch (NumberFormatException e) {
                    LOG.warn("Unable to parse double value for property: " + propertyValue);
                }
            }
            return (new DoubleWritable(vertexValue));
        }

        /**
         * Add edges to Giraph vertex.
         *
         * @param vertex       Giraph vertex to add edges to
         * @param faunusVertex (Faunus (Titan/Hadoop) vertex
         * @param titanEdges   Iterator of Titan edges
         */
        private void addGiraphEdges(Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
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
        private Edge<LongWritable, NullWritable> getGiraphEdge(FaunusVertex faunusVertex, TitanEdge titanEdge) {
            return EdgeFactory.create(new LongWritable(
                    titanEdge.getOtherVertex(faunusVertex).getLongId()), NullWritable.get());
        }

    }
}
