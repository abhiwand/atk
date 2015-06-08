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

import com.intel.giraph.io.EdgeData4CFWritable;
import com.intel.giraph.io.VertexData4CFWritable;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.Iterator;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_DATA_ERROR;


/**
 * TitanHBaseVertexInputFormatPropertyGraph4CF loads vertex
 * Features <code>VertexData</code> vertex values and
 * <code>EdgeData</code> out-edge info.
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
public class TitanVertexInputFormatPropertyGraph4CF extends
        TitanVertexInputFormat<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> {

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
    public VertexReader<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {

        return new PropertyGraph4CFVertexReader(split, context);
    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class PropertyGraph4CFVertexReader extends TitanVertexReaderCommon<VertexData4CFWritable, EdgeData4CFWritable> {

        /**
         * The length of vertex value vector
         */
        private int cardinality = -1;

        /**
         * Constructs vertex reader
         *
         * @param split   Input split from HBase table
         * @param context Giraph task context
         * @throws IOException
         */
        public PropertyGraph4CFVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            super(split, context);
        }

        /**
         * Get vertex value
         *
         * @return VertexDataWritable vertex value in vector
         * @throws IOException
         */
        protected VertexData4CFWritable getValue() throws IOException {
            VertexData4CFWritable vertexValue = giraphVertex.getValue();
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
        @Override
        public Vertex<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {

            // Initialize Giraph vertex
            Vertex<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> vertex = conf.createVertex();
            VertexData4CFWritable.VertexType vertexType = vertexBuilder.getCFVertexTypeProperty(
                    faunusVertex, vertexBuilder.vertexTypePropertyKey);
            initializeVertexProperties(vertex, faunusVertex, vertexType, vertexBuilder.vertexValuePropertyKeys.size());

            // Update vertex properties
            Iterator<TitanProperty> titanProperties = vertexBuilder.buildTitanProperties(faunusVertex);
            setVertexProperties(vertex, vertexType, titanProperties);

            // Add edges
            Iterator<TitanEdge> titanEdges = vertexBuilder.buildBlueprintsEdges(faunusVertex);
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
                Vertex<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> vertex,
                FaunusVertex faunusVertex,
                VertexData4CFWritable.VertexType vertexType,
                int size) {
            double[] data = new double[size];
            Vector priorVector = new DenseVector(data);
            VertexData4CFWritable vertexValueVector = new VertexData4CFWritable(vertexType, priorVector.clone());
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
        private void setVertexProperties(Vertex<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> vertex,
                                         VertexData4CFWritable.VertexType vertexType, Iterator<TitanProperty> titanProperties) {
            while (titanProperties.hasNext()) {
                TitanProperty titanProperty = titanProperties.next();
                Vector priorVector = vertexBuilder.setVector(vertex.getValue().getVector(), titanProperty);
                vertex.setValue(new VertexData4CFWritable(vertexType, priorVector));
            }
        }

        @Override
        public <A extends Writable> A getAggregatedValue(String name) {
            return super.getAggregatedValue(name);
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
        @Override
         public void addGiraphEdges(Vertex<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> vertex,
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
