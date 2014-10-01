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
import com.intel.giraph.io.VertexData4LBPWritable;
import com.intel.giraph.io.VertexData4LDAWritable;
import com.intel.mahout.math.DoubleWithVectorWritable;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
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
 * TitanHBaseVertexInputFormatPropertyGraph4LDA loads vertex from Titan
 * Features <code>VertexData4LDAWritable</code> vertex values and
 * <code>DoubleWithVectorWritable</code> out-edge info.
 * <p/>
 * Each vertex follows this format:
 * (<vertex id>, <vertex valueVector>, <vertex property>,
 * ((<dest vertex id>, <edge value>, <edge property>), ...))
 * <p/>
 * Here is an example of left-side vertex, with vertex id 1,
 * vertex value 4,3 marked as "d", and two edges.
 * First edge has a destination vertex 2, edge value 2.1.
 * Second edge has a destination vertex 3, edge value 0.7.
 * [1,[4,3],[d],[[2,2.1,[]],[3,0.7,[]]]]
 */
public class TitanHBaseVertexInputFormatPropertyGraph4LDA extends
        TitanHBaseVertexInputFormat<LongWritable,
                VertexData4LDAWritable, DoubleWithVectorWritable> {

    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexInputFormatPropertyGraph4LDA.class);

    /**
     * create TitanHBaseVertexReader
     *
     * @param split   : inputsplits from TableInputFormat
     * @param context : task context
     * @return VertexReader
     * @throws IOException
     * @throws RuntimeException
     */
    @Override
    public VertexReader<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {

        return new PropertyGraph4LDAVertexReader(split, context);
    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class PropertyGraph4LDAVertexReader extends TitanHBaseVertexReader<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> {

        private Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> giraphVertex;

        /**
         * The length of vertex value vector
         */
        private int cardinality = -1;

        /**
         * TitanHBaseVertexReader constructor
         *
         * @param split   Input Split
         * @param context Task context
         * @throws IOException
         */
        public PropertyGraph4LDAVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
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
         * @return Giraph vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> getCurrentVertex()
                throws IOException, InterruptedException {
            return giraphVertex;
        }

        /**
         * Get edges of this vertex
         *
         * @return Iterable of Giraph edges
         * @throws IOException
         */
        protected Iterable<Edge<LongWritable, DoubleWithVectorWritable>> getEdges() throws IOException {
            return giraphVertex.getEdges();
        }

        /**
         * Get vertex value
         *
         * @return TwoVectorWritable vertex value in two vectors
         * @throws IOException
         */
        protected VertexData4LDAWritable getValue() throws IOException {
            VertexData4LDAWritable vertexValue = giraphVertex.getValue();
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
         * Construct a Giraph vertex from a Faunus (Titan/Hadoop vertex).
         *
         * @param conf         Giraph configuration with property names, and edge labels to filter
         * @param faunusVertex Faunus vertex
         * @return Giraph vertex
         */
        private Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {

            // set up vertex Value
            Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex = conf.createVertex();
            VertexData4LDAWritable.VertexType vertexType = getVertexTypeProperty(faunusVertex, vertexBuilder.vertexTypePropertyKey);
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
         * Initialize Giraph vertex with vertex type and  vector.
         *
         * @param vertex       Giraph vertex
         * @param faunusVertex Faunus (Titan/Hadoop) vertex
         * @param vertexType   Vertex type (Left or Right)
         * @param size         Size of prior vector
         */
        private void initializeVertexProperties(
                Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex,
                FaunusVertex faunusVertex,
                VertexData4LDAWritable.VertexType vertexType,
                int size) {
            double[] data = new double[size];
            Vector vector = new DenseVector(data);
            VertexData4LDAWritable vertexValueVector = new VertexData4LDAWritable(vertexType, vector.clone());
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), vertexValueVector);
        }

        /**
         * Update vector in Giraph vertex.
         * <p/>
         * The prior vector is updated using the values in the Titan properties. The posterior is
         * initialized to zero vector.
         *
         * @param vertex          Giraph vertex
         * @param vertexType      Vertex type (Left, Right)
         * @param titanProperties Iterator of Titan properties
         */
        private void setVertexProperties(Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex,
                                         VertexData4LDAWritable.VertexType vertexType, Iterator<TitanProperty> titanProperties) {
            while (titanProperties.hasNext()) {
                TitanProperty titanProperty = titanProperties.next();
                Vector vector = vertexBuilder.setVector(vertex.getValue().getVector(), titanProperty);
                vertex.setValue(new VertexData4LDAWritable(vertexType, vector));
            }
        }

        /**
         * Get LDA vertex property type
         *
         * @param faunusVertex          Faunus (Titan/Hadoop) vertex
         * @param vertexTypePropertyKey Property name for vertex type
         * @return LBP Vertex property type (Train, Test, Validate)
         * @throws IllegalArgumentException
         */
        private VertexData4LDAWritable.VertexType getVertexTypeProperty(FaunusVertex faunusVertex,
                                                                        String vertexTypePropertyKey)
                throws IllegalArgumentException {

            VertexData4LDAWritable.VertexType vertexType;
            Object vertexTypeObject = faunusVertex.getProperty(vertexTypePropertyKey);
            String vertexTypeString = vertexTypeObject.toString().toLowerCase();

            if (vertexTypeObject != null) {
                if (vertexTypeString.equals(VERTEX_TYPE_LEFT)) {
                    vertexType = VertexData4LDAWritable.VertexType.LEFT;
                } else if (vertexTypeString.equals(VERTEX_TYPE_RIGHT)) {
                    vertexType = VertexData4LDAWritable.VertexType.RIGHT;
                } else {
                    LOG.error("Vertex type string: %s isn't supported." + vertexTypeString);
                    throw new IllegalArgumentException(String.format(
                            "Vertex type string: %s isn't supported.", vertexTypeString));
                }
            } else {
                LOG.error(String.format("Vertex type property %s should not be null", vertexTypePropertyKey));
                throw new IllegalArgumentException(String.format(
                        "Vertex type property %s should not be null", vertexTypePropertyKey));
            }
            return (vertexType);
        }


        /**
         * Add edges to Giraph vertex.
         *
         * @param vertex       Giraph vertex
         * @param faunusVertex Faunus (Titan/Hadoop) vertex
         * @param titanEdges   Iterator of Titan edges
         */
        private void addGiraphEdges(Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex,
                                    FaunusVertex faunusVertex, Iterator<TitanEdge> titanEdges) {
            while (titanEdges.hasNext()) {
                TitanEdge titanEdge = titanEdges.next();

                for (final String propertyKey : vertexBuilder.edgeValuePropertyKeys.keySet()) {
                    final Object edgeValueObject = titanEdge.getProperty(propertyKey);
                    double edgeValue = Double.parseDouble(edgeValueObject.toString());
                    Edge<LongWritable, DoubleWithVectorWritable> edge = EdgeFactory.create(
                            new LongWritable(titanEdge.getOtherVertex(faunusVertex).getLongId()), new DoubleWithVectorWritable(
                                    edgeValue, new DenseVector()));
                    vertex.addEdge(edge);
                }
            }
        }
    }
}
