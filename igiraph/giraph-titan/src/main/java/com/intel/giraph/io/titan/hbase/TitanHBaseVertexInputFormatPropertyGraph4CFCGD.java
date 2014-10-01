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

import com.intel.giraph.io.EdgeData4CFWritable;
import com.intel.giraph.io.VertexData4CFWritable;
import com.intel.giraph.io.VertexData4CGDWritable;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
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
 * TitanHBaseVertexInputFormatPropertyGraph4CFCGD loads vertex
 * Features <code>VertexData4CGD</code> vertex values and
 * <code>EdgeDataWritable</code> out-edge info.
 * <p/>
 * Each vertex follows this format:
 * (<vertex id>, <vertex valueVector>, <vertex property>,
 * ((<dest vertex id>, <edge value>, <edge property>), ...))
 * <p/>
 * Here is an example of left-side vertex, with vertex id 1,
 * vertex value 4,3 marked as "l", and two edges.
 * First edge has a destination vertex 2, edge value 2.1, marked as "tr".
 * Second edge has a destination vertex 3, edge value 0.7,marked as "va".
 * [1,[4,3],[L],[[2,2.1,[tr]],[3,0.7,[va]]]]
 */
public class TitanHBaseVertexInputFormatPropertyGraph4CFCGD extends
        TitanHBaseVertexInputFormat<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> {

    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexInputFormatPropertyGraph4CFCGD.class);

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
    public VertexReader<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new PropertyGraph4CFCGDVertexReader(split, context);
    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class PropertyGraph4CFCGDVertexReader extends
            TitanHBaseVertexReader<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> {

        private Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> giraphVertex;

        /**
         * The length of vertex value vector
         */
        private int cardinality = -1;

        /**
         * Constructs Giraph vertex reader
         * <p/>
         * Reads Giraph vertex from Titan/HBase table.
         *
         * @param split   Input split from HBase table
         * @param context Giraph task context
         * @throws IOException
         */
        public PropertyGraph4CFCGDVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
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
        public Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> getCurrentVertex()
                throws IOException, InterruptedException {
            return giraphVertex;
        }

        /**
         * Get value of Giraph vertex
         *
         * @return VertexData4CGDWritable vertex value in vector
         * @throws IOException
         */
        protected VertexData4CGDWritable getValue() throws IOException {
            VertexData4CGDWritable vertexValue = giraphVertex.getValue();
            Vector vector = vertexValue.getVector();
            if (cardinality != vector.size()) {
                if (cardinality == -1) {
                    cardinality = vector.size();
                } else {
                    throw new IllegalArgumentException(INPUT_DATA_ERROR);
                }
            }
            return vertexValue;
        }

        /**
         * Get edges of Giraph vertex
         *
         * @return Iterable of Giraph edges
         * @throws IOException
         */
        protected Iterable<Edge<LongWritable, EdgeData4CFWritable>> getEdges() throws IOException {
            return giraphVertex.getEdges();
        }

        /**
         * Construct a Giraph vertex from a Faunus (Titan/Hadoop vertex).
         *
         * @param conf         Giraph configuration with property names, and edge labels to filter
         * @param faunusVertex Faunus vertex
         * @return Giraph vertex
         */
        private Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {

            // Initialize Giraph vertex
            Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> vertex = conf.createVertex();
            VertexData4CFWritable.VertexType vertexType = vertexBuilder.getCFVertexTypeProperty(
                    faunusVertex, vertexBuilder.vertexTypePropertyKey);
            initializeVertexProperties(vertex, faunusVertex, vertexType, vertexBuilder.vertexValuePropertyKeys.size());

            // Update vertex properties
            Iterator<TitanProperty> titanProperties = vertexBuilder.buildTitanProperties(faunusVertex);
            setVertexProperties(vertex, vertexType, titanProperties);

            // Add edges
            Iterator<TitanEdge> titanEdges = vertexBuilder.buildTitanEdges(faunusVertex);
            addGiraphEdges(vertex, faunusVertex, titanEdges);

            return (vertex);
        }

        /**
         * Initialize Giraph vertex with vertex type and prior vector.
         *
         * @param vertex       Giraph vertex
         * @param faunusVertex Faunus (Titan/Hadoop) vertex
         * @param vertexType   Vertex type (Left or Right)
         * @param size         Size of prior vector
         */
        private void initializeVertexProperties(
                Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> vertex,
                FaunusVertex faunusVertex,
                VertexData4CFWritable.VertexType vertexType,
                int size) {
            double[] data = new double[size];
            Vector priorVector = new DenseVector(data);
            VertexData4CGDWritable vertexValueVector = new VertexData4CGDWritable(vertexType, priorVector.clone(),
                    priorVector.clone(), priorVector.clone());
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), vertexValueVector);
        }

        /**
         * Update prior vector in Giraph vertex.
         * <p/>
         * The prior vector is updated using the values in the Titan properties.
         *
         * @param vertex          Giraph vertex
         * @param vertexType      Vertex type (Left or Right)
         * @param titanProperties Iterator of Titan properties
         */
        private void setVertexProperties(Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> vertex,
                                         VertexData4CFWritable.VertexType vertexType, Iterator<TitanProperty> titanProperties) {
            while (titanProperties.hasNext()) {
                TitanProperty titanProperty = titanProperties.next();
                Vector priorVector = vertexBuilder.setVector(vertex.getValue().getVector(), titanProperty);
                vertex.setValue(new VertexData4CGDWritable(vertexType, priorVector,
                        priorVector.clone(), priorVector.clone()));
            }
        }

        /**
         * Add edges to Giraph vertex.
         * <p/>
         * Edges contain edge type, and value.
         *
         * @param vertex       Giraph vertex
         * @param faunusVertex Faunus (Titan/Hadoop) vertex
         * @param titanEdges   Iterator of Titan edges
         */
        private void addGiraphEdges(Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> vertex,
                                    FaunusVertex faunusVertex, Iterator<TitanEdge> titanEdges) {
            while (titanEdges.hasNext()) {
                TitanEdge titanEdge = titanEdges.next();
                EdgeData4CFWritable.EdgeType edgeType = vertexBuilder.getCFEdgeTypeProperty(titanEdge, vertexBuilder.edgeTypePropertyKey);

                for (final String propertyKey : vertexBuilder.edgeValuePropertyKeys.keySet()) {
                    Edge<LongWritable, EdgeData4CFWritable> edge = vertexBuilder.getCFGiraphEdge(faunusVertex, titanEdge, edgeType, propertyKey);
                    vertex.addEdge(edge);
                }
            }
        }

    }
}
