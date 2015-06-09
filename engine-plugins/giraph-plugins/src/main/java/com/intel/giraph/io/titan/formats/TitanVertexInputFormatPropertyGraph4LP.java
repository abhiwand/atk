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
public class TitanVertexInputFormatPropertyGraph4LP extends
        TitanVertexInputFormat<LongWritable, VertexData4LPWritable, DoubleWritable> {

    private static final Logger LOG = Logger.getLogger(TitanVertexInputFormatPropertyGraph4LP.class);

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
    public static class PropertyGraph4LPVertexReader extends TitanVertexReaderCommon<VertexData4LPWritable, DoubleWritable> {

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
            super(split, context);
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
        @Override
        public void addGiraphEdges(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex,
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
        @Override
        public Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {

            Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex = conf.createVertex();
            initializeVertexProperties(vertex, faunusVertex, vertexBuilder.vertexValuePropertyKeys.size());

            // Update vertex properties
            Iterator<TitanProperty> titanProperties = vertexBuilder.buildTitanProperties(faunusVertex);
            setVertexProperties(vertex, titanProperties);

            // Add edges
            Iterator<TitanEdge> titanEdges = vertexBuilder.buildBlueprintsEdges(faunusVertex);
            addGiraphEdges(vertex, faunusVertex, titanEdges);

            return (vertex);
        }
    }
}
