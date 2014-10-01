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

import com.intel.giraph.io.VertexData4LPWritable;
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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.Iterator;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_DATA_ERROR;

/**
 * TitanHBaseVertexInputFormatPropertyGraph4LP loads vertex
 * with <code>long</code> vertex ID's,
 * <code>TwoVector</code> vertex values: one for prior and
 * one for posterior,
 * and <code>DoubleVector</code> edge weights.
 */
public class TitanHBaseVertexInputFormatPropertyGraph4LP extends
        TitanHBaseVertexInputFormat<LongWritable, VertexData4LPWritable, DoubleWritable> {

    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexInputFormatPropertyGraph4LP.class);

    /**
     * create TitanHBaseVertexReader
     *
     * @param split   : inputsplits from TableInputFormat
     * @param context : task context
     * @return VertexReader
     * @throws IOException
     * @throws RuntimeException
     */
    public VertexReader<LongWritable, VertexData4LPWritable, DoubleWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new PropertyGraph4LPVertexReader(split, context);
    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class PropertyGraph4LPVertexReader extends TitanHBaseVertexReader<LongWritable, VertexData4LPWritable, DoubleWritable> {

        private Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> giraphVertex = null;

        /**
         * The length of vertex value vector
         */
        private int cardinality = 0;

        /**
         * TitanHBaseVertexReader constructor
         *
         * @param split   Input Split
         * @param context Task context
         * @throws IOException
         */
        public PropertyGraph4LPVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
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
         * Get current vertex with ID in long; value as two vectors, both in
         * double edge as two vectors, both in double
         *
         * @return Vertex Giraph vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> getCurrentVertex()
                throws IOException, InterruptedException {
            return giraphVertex;
        }

        /**
         * Get vertex value
         *
         * @return TwoVectorWritable vertex value in two vectors
         * @throws IOException
         */
        protected VertexData4LPWritable getValue() throws IOException {
            VertexData4LPWritable vertexValue = giraphVertex.getValue();
            Vector priorVector = vertexValue.getPriorVector();
            if (cardinality != priorVector.size()) {
                if (cardinality == 0) {
                    cardinality = priorVector.size();
                } else {
                    throw new IllegalArgumentException(INPUT_DATA_ERROR);
                }
            }
            return vertexValue;
        }

        /**
         * Get edges of this vertex
         *
         * @return Iterable of Giraph edges
         * @throws IOException
         */
        protected Iterable<Edge<LongWritable, DoubleWritable>> getEdges() throws IOException {
            return giraphVertex.getEdges();
        }

        /**
         * Initialize Giraph vertex with vector.
         *
         * @param vertex       Giraph vertex
         * @param faunusVertex Faunus (Titan/Hadoop) vertex
         * @param size         Size of prior vector
         */
        private void initializeVertexProperties(
                Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex,
                FaunusVertex faunusVertex,
                int size) {
            double[] data = new double[size];
            Vector vector = new DenseVector(data);
            VertexData4LPWritable vertexValueVector = new VertexData4LPWritable(vector.clone(), vector.clone(), 0d);
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), vertexValueVector);
        }

        /**
         * Update vectors in Giraph vertex.
         * <p/>
         * The prior vector is updated using the values in the Titan properties. The posterior is
         * initialized to zero vector.
         *
         * @param vertex          Giraph vertex
         * @param titanProperties Iterator of Titan properties
         */
        private void setVertexProperties(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex,
                                         Iterator<TitanProperty> titanProperties) {
            while (titanProperties.hasNext()) {
                TitanProperty titanProperty = titanProperties.next();
                Vector priorVector = vertexBuilder.setVector(vertex.getValue().getPriorVector(), titanProperty);
                Vector vector = new DenseVector(new double[priorVector.size()]);
                vertex.setValue(new VertexData4LPWritable(priorVector, vector.clone(), 0d));
            }
        }


        /**
         * Add edges to Giraph vertex.
         *
         * @param vertex       Giraph vertex
         * @param faunusVertex Faunus (Titan/Hadoop) vertex
         * @param titanEdges   Iterator of Titan edges
         */
        private void addGiraphEdges(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex,
                                    FaunusVertex faunusVertex, Iterator<TitanEdge> titanEdges) {
            while (titanEdges.hasNext()) {
                TitanEdge titanEdge = titanEdges.next();

                for (final String propertyKey : vertexBuilder.edgeValuePropertyKeys.keySet()) {
                    final Object edgeValueObject = titanEdge.getProperty(propertyKey);
                    double edgeValue = Double.parseDouble(edgeValueObject.toString());
                    Edge<LongWritable, DoubleWritable> edge = EdgeFactory.create(
                            new LongWritable(titanEdge.getOtherVertex(faunusVertex).getLongId()), new DoubleWritable(
                                    edgeValue));
                    vertex.addEdge(edge);
                }
            }
        }

        /**
         * Construct a Giraph vertex from a Faunus (Titan/Hadoop vertex).
         *
         * @param conf         Giraph configuration with property names, and edge labels to filter
         * @param faunusVertex Faunus vertex
         * @return Giraph vertex
         */
        private Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {

            Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex = conf.createVertex();
            initializeVertexProperties(vertex, faunusVertex, vertexBuilder.vertexValuePropertyKeys.size());

            // Update vertex properties
            Iterator<TitanProperty> titanProperties = vertexBuilder.buildTitanProperties(faunusVertex);
            setVertexProperties(vertex, titanProperties);

            // Add edges
            Iterator<TitanEdge> titanEdges = vertexBuilder.buildTitanEdges(faunusVertex);
            addGiraphEdges(vertex, faunusVertex, titanEdges);

            return (vertex);
        }
    }
}
