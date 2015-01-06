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
package com.intel.giraph.io.titan.formats;

import com.intel.giraph.io.EdgeData4GBPWritable;
import com.intel.giraph.io.GaussianDistWritable;
import com.intel.giraph.io.VertexData4GBPWritable;
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


public class TitanVertexInputFormatPropertyGraph4GBP extends
        TitanVertexInputFormat<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> {

    private static final Logger LOG = Logger.getLogger(TitanVertexInputFormatPropertyGraph4GBP.class);

    /**
     * Constructs Giraph vertex reader
     * Reads Giraph vertex from Titan/HBase table.
     *
     * @param split   Input split from HBase table
     * @param context Giraph task context
     * @throws IOException
     */
    @Override
    public VertexReader<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new PropertyGraph4GBPVertexReader(split, context);
    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class PropertyGraph4GBPVertexReader extends
            TitanVertexReader<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> {

        private Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> giraphVertex = null;

        /**
         * The length of vertex value vector
         */
        //private int cardinality = 0;

        /**
         * Constructs Giraph vertex reader
         * <p/>
         * Reads Giraph vertex from Titan/HBase table.
         *
         * @param split   Input split from HBase table
         * @param context Giraph task context
         * @throws IOException
         */
        public PropertyGraph4GBPVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            super(split, context);
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
            while (getRecordReader().nextKeyValue()) {
                Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable>  tempGiraphVertex =
                        readGiraphVertex(getConf(), getRecordReader().getCurrentValue());
                if (tempGiraphVertex != null) {
                    this.giraphVertex = tempGiraphVertex;
                    return true;
                }
            }
            this.giraphVertex = null;
            return false;

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
        public Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> getCurrentVertex()
                throws IOException, InterruptedException {
            return giraphVertex;
        }

        /**
         * Get Giraph vertex value
         *
         * @return TwoVectorWritable vertex value in two vectors
         * @throws IOException
         */
        protected VertexData4GBPWritable getValue() throws IOException {
            VertexData4GBPWritable vertexValue = giraphVertex.getValue();

            return vertexValue;
        }

        /**
         * Get edges of Giraph vertex
         *
         * @return Iterable of Giraph edges
         * @throws IOException
         */
        protected Iterable<Edge<LongWritable, EdgeData4GBPWritable>> getEdges() throws IOException {
            return giraphVertex.getEdges();
        }

        /**
         * Construct a Giraph vertex from a Faunus (Titan/Hadoop vertex).
         *
         * @param conf         Giraph configuration with property names, and edge labels to filter
         * @param faunusVertex Faunus vertex
         * @return Giraph vertex
         */
        public Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {

            // Initialize Giraph vertex
            Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex = conf.createVertex();
            initializeVertexProperties(vertex, faunusVertex, vertexBuilder.vertexValuePropertyKeys.size());

            // Update vertex properties
            Iterator<TitanProperty> titanProperties = vertexBuilder.buildTitanProperties(faunusVertex);
            setVertexProperties(vertex, titanProperties);

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
         * @param size         Size of prior vector
         */
        private void initializeVertexProperties(
                Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex,
                FaunusVertex faunusVertex,
                int size) {
            double[] data = new double[size];
            //TODO: Check what mean and precision should be
            double mean = 0d;
            double precision = 0.0;
            GaussianDistWritable priorVector = new GaussianDistWritable(mean, precision);
            GaussianDistWritable posteriorVector = new GaussianDistWritable();
            VertexData4GBPWritable vertexValueVector = new VertexData4GBPWritable(
                    priorVector, posteriorVector, priorVector,mean);
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), vertexValueVector);
        }

        /**
         * Update prior and posterior vector in Giraph vertex.
         * <p/>
         * The prior vector is updated using the values in the Titan properties. The posterior is
         * initialized to zero vector.
         *
         * @param vertex          Giraph vertex
         * @param titanProperties Iterator of Titan properties
         */
        private void setVertexProperties(Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex,
                                          Iterator<TitanProperty> titanProperties) {
            while (titanProperties.hasNext()) {
                TitanProperty titanProperty = titanProperties.next();
                GaussianDistWritable priorVector = vertex.getValue().getPrior();
                GaussianDistWritable posteriorVector = vertex.getValue().getPosterior();
                GaussianDistWritable intermediateVector = vertex.getValue().getIntermediate();
                Double mean = vertex.getValue().getPrevMean();
                //TODO: Check do we need the titanProperty?
                //Vector priorVector = vertexBuilder.setVector(vertex.getValue().getPriorVector(), titanProperty);
                //Vector posteriorVector = vertexBuilder.setVector(vertex.getValue().getPosteriorVector(), titanProperty);
                //Vector intermediateVector = vertexBuilder.setVector(vertex.getValue().getIntermediateVector(), titanProperty);

                vertex.setValue(new VertexData4GBPWritable(priorVector, posteriorVector, intermediateVector, mean));
            }
        }

        /**
         * Add edges to Giraph vertex.
         *
         * @param vertex       Giraph vertex
         * @param faunusVertex Faunus (Titan/Hadoop) vertex
         * @param titanEdges   Iterator of Titan edges
         */
        private void addGiraphEdges(Vertex<LongWritable, VertexData4GBPWritable, EdgeData4GBPWritable> vertex,
                                    FaunusVertex faunusVertex, Iterator<TitanEdge> titanEdges) {
            while (titanEdges.hasNext()) {
                TitanEdge titanEdge = titanEdges.next();

                for (final String propertyKey : vertexBuilder.edgeValuePropertyKeys.keySet()) {
                    final Object edgeValueObject = titanEdge.getProperty(propertyKey);
                    double edgeValue = Double.parseDouble(edgeValueObject.toString());
                    Edge<LongWritable, EdgeData4GBPWritable> edge = EdgeFactory.create(
                            new LongWritable(titanEdge.getOtherVertex(faunusVertex).getLongId()), new EdgeData4GBPWritable());
                    //TODO: Check edge initializations
                       vertex.addEdge(edge);
                }
            }
        }


    }
}
