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

import com.intel.giraph.io.VertexData4LBPWritable;
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

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;


/**
 * TitanHBaseVertexInputFormatPropertyGraph4LBP loads vertex
 * with <code>VertexData4LBP</code> vertex value
 * and <code>double</code> out-edge weights.
 * <p/>
 * Each vertex follows this format:
 * (<vertex id>, <vertex valueVector>, <vertex type>,
 * ((<dest vertex id>, <edge value>, <edge type>), ...))
 * <p/>
 * Here is an example with vertex id 1, vertex value 4,3, and two edges.
 * First edge has a destination vertex 2, edge value 2.1.
 * Second edge has a destination vertex 3, edge value 0.7.
 * [1,[4,3],["TR"],[[2,2.1,[]],[3,0.7,[]]]], where the empty edge-type
 * '[]' is reserved for future usage.
 */
public class TitanHBaseVertexInputFormatPropertyGraph4LBP extends
        TitanHBaseVertexInputFormat<LongWritable, VertexData4LBPWritable, DoubleWritable> {

    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexInputFormatPropertyGraph4LBP.class);

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
    public VertexReader<LongWritable, VertexData4LBPWritable, DoubleWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new PropertyGraph4LBPVertexReader(split, context);
    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class PropertyGraph4LBPVertexReader extends
            TitanHBaseVertexReader<LongWritable, VertexData4LBPWritable, DoubleWritable> {

        private Vertex<LongWritable, VertexData4LBPWritable, DoubleWritable> giraphVertex = null;

        /**
         * The length of vertex value vector
         */
        private int cardinality = 0;

        /**
         * Constructs Giraph vertex reader
         * <p/>
         * Reads Giraph vertex from Titan/HBase table.
         *
         * @param split   Input split from HBase table
         * @param context Giraph task context
         * @throws IOException
         */
        public PropertyGraph4LBPVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
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
        public Vertex<LongWritable, VertexData4LBPWritable, DoubleWritable> getCurrentVertex()
                throws IOException, InterruptedException {
            return giraphVertex;
        }

        /**
         * Get Giraph vertex value
         *
         * @return TwoVectorWritable vertex value in two vectors
         * @throws IOException
         */
        protected VertexData4LBPWritable getValue() throws IOException {
            VertexData4LBPWritable vertexValue = giraphVertex.getValue();
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
         * Get edges of Giraph vertex
         *
         * @return Iterable of Giraph edges
         * @throws IOException
         */
        protected Iterable<Edge<LongWritable, DoubleWritable>> getEdges() throws IOException {
            return giraphVertex.getEdges();
        }

        /**
         * Construct a Giraph vertex from a Faunus (Titan/Hadoop vertex).
         *
         * @param conf         Giraph configuration with property names, and edge labels to filter
         * @param faunusVertex Faunus vertex
         * @return Giraph vertex
         */
        public Vertex<LongWritable, VertexData4LBPWritable, DoubleWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {

            // Initialize Giraph vertex
            Vertex<LongWritable, VertexData4LBPWritable, DoubleWritable> vertex = conf.createVertex();
            VertexData4LBPWritable.VertexType vertexType = getLBPVertexTypeProperty(
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
                Vertex<LongWritable, VertexData4LBPWritable, DoubleWritable> vertex,
                FaunusVertex faunusVertex,
                VertexData4LBPWritable.VertexType vertexType,
                int size) {
            double[] data = new double[size];
            Vector priorVector = new DenseVector(data);
            VertexData4LBPWritable vertexValueVector = new VertexData4LBPWritable(vertexType,
                    priorVector.clone(), priorVector.clone());
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), vertexValueVector);
        }

        /**
         * Update prior and posterior vector in Giraph vertex.
         * <p/>
         * The prior vector is updated using the values in the Titan properties. The posterior is
         * initialized to zero vector.
         *
         * @param vertex          Giraph vertex
         * @param vertexType      Vertex type (Train, Validate, Test)
         * @param titanProperties Iterator of Titan properties
         */
        private void setVertexProperties(Vertex<LongWritable, VertexData4LBPWritable, DoubleWritable> vertex,
                                         VertexData4LBPWritable.VertexType vertexType, Iterator<TitanProperty> titanProperties) {
            while (titanProperties.hasNext()) {
                TitanProperty titanProperty = titanProperties.next();
                Vector priorVector = vertexBuilder.setVector(vertex.getValue().getPriorVector(), titanProperty);
                Vector posteriorVector = new DenseVector(new double[priorVector.size()]);
                vertex.setValue(new VertexData4LBPWritable(vertexType, priorVector, posteriorVector));
            }
        }

        /**
         * Add edges to Giraph vertex.
         *
         * @param vertex       Giraph vertex
         * @param faunusVertex Faunus (Titan/Hadoop) vertex
         * @param titanEdges   Iterator of Titan edges
         */
        private void addGiraphEdges(Vertex<LongWritable, VertexData4LBPWritable, DoubleWritable> vertex,
                                    FaunusVertex faunusVertex, Iterator<TitanEdge> titanEdges) {
            while (titanEdges.hasNext()) {
                TitanEdge titanEdge = titanEdges.next();

                for (final String propertyKey : vertexBuilder.edgeValuePropertyKeys.keySet()) {
                    final Object edgeValueObject = titanEdge.getProperty(propertyKey);
                    double edgeValue = Double.parseDouble(edgeValueObject.toString());
                    Edge<LongWritable, DoubleWritable> edge = EdgeFactory.create(
                            new LongWritable(titanEdge.getOtherVertex(faunusVertex).getLongId()), new DoubleWritable(edgeValue));
                    vertex.addEdge(edge);
                }
            }
        }

        /**
         * Get LBP vertex property type
         *
         * @param faunusVertex          Faunus (Titan/Hadoop) vertex
         * @param vertexTypePropertyKey Property name for vertex type
         * @return LBP Vertex property type (Train, Test, Validate)
         * @throws IllegalArgumentException
         */
        private VertexData4LBPWritable.VertexType getLBPVertexTypeProperty(FaunusVertex faunusVertex,
                                                                           String vertexTypePropertyKey)
                throws IllegalArgumentException {
            VertexData4LBPWritable.VertexType vertexType;
            Object vertexTypeObject = faunusVertex.getProperty(vertexTypePropertyKey);

            if (vertexTypeObject != null) {
                String vertexTypeString = vertexTypeObject.toString().toLowerCase();
                if (vertexTypeString.equals(TYPE_TRAIN)) {
                    vertexType = VertexData4LBPWritable.VertexType.TRAIN;
                } else if (vertexTypeString.equals(TYPE_VALIDATE)) {
                    vertexType = VertexData4LBPWritable.VertexType.VALIDATE;
                } else if (vertexTypeString.equals(TYPE_TEST)) {
                    vertexType = VertexData4LBPWritable.VertexType.TEST;
                } else {
                    LOG.error(WRONG_VERTEX_TYPE + vertexTypeString);
                    throw new IllegalArgumentException(String.format(
                            WRONG_VERTEX_TYPE, vertexTypeString));
                }
            } else {
                LOG.error(String.format("Vertex type property %s should not be null", vertexTypePropertyKey));
                throw new IllegalArgumentException(String.format(
                        "Vertex type property %s should not be null", vertexTypePropertyKey));
            }
            return (vertexType);
        }


    }
}
