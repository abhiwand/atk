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
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import com.intel.giraph.io.EdgeData4CFWritable;
import com.intel.giraph.io.EdgeData4CFWritable.EdgeType;
import com.intel.giraph.io.VertexData4CFWritable.VertexType;
import com.intel.giraph.io.VertexData4CGDWritable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.TYPE_TEST;


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

    /**
     * LOG class
     */
    private static final Logger LOG = Logger
        .getLogger(TitanHBaseVertexInputFormatPropertyGraph4CFCGD.class);

    /**
     * checkInputSpecs
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void checkInputSpecs(Configuration conf) {
    }

    /**
     * set up HBase with based on users' configuration
     *
     * @param conf :Giraph configuration
     */
    @Override
    public void setConf(
        ImmutableClassesGiraphConfiguration<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> conf) {
        GiraphTitanUtils.setupHBase(conf);
        super.setConf(conf);
    }

    /**
     * create TitanHBaseVertexReader
     *
     * @param split   : inputsplits from TableInputFormat
     * @param context : task context
     * @return VertexReader
     * @throws IOException
     * @throws RuntimeException
     */
    public VertexReader<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> createVertexReader(
        InputSplit split, TaskAttemptContext context) throws IOException {

        return new TitanHBaseVertexReader(split, context);

    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class TitanHBaseVertexReader extends
        HBaseVertexReader<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> {
        /**
         * reader to parse Titan graph
         */
        private TitanHBaseGraphReader graphReader;
        /**
         * Giraph vertex
         */
        private Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> vertex;
        /**
         * task context
         */
        private final TaskAttemptContext context;
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
        public TitanHBaseVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            this.context = context;
        }

        /**
         * initialize TitanHBaseVertexReader
         *
         * @param inputSplit input splits
         * @param context    task context
         */
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
            InterruptedException {
            super.initialize(inputSplit, context);
            this.graphReader = new TitanHBaseGraphReader(
                GiraphToTitanGraphFactory.generateTitanConfiguration(context.getConfiguration(),
                    GIRAPH_TITAN.get(context.getConfiguration())));
        }

        /**
         * check whether there is next vertex
         *
         * @return boolean true if there is next vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            //the edge store name used by Titan
            if (getRecordReader().nextKeyValue()) {
                vertex = readGiraphVertex(getConf(), getRecordReader().getCurrentValue());
                return true;
            }

            vertex = null;
            return false;
        }


        public Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> readGiraphVertex(final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {
            /** Vertex value properties to filter */
            final String[] vertexValuePropertyKeyList;
            /** Edge value properties to filter */
            final String[] edgeValuePropertyKeyList;
            /** Edge labels to filter */
            final String[] edgeLabelList;

            String regexp = "[\\s,\\t]+";
            /**
             * vertex value
             */
            VertexData4CGDWritable vertexValueVector = null;
            /**
             * the vertex type
             */
            VertexType vertexType = null;
            /**
             * the edge type
             */
            EdgeData4CFWritable.EdgeType edgeType = null;
            /**
             * Enable vector value
             */
            String enableVectorValue = "true";

            /**
             * Property key for Vertex Type
             */
            String vertexTypePropertyKey;
            /**
             * Property key for Edge Type
             */
            String edgeTypePropertyKey;

            /**
             * HashMap of configured vertex properties
             */
            final Map<String, Integer> vertexValuePropertyKeys = new HashMap<String, Integer>();
            /**
             * HashMap of configured edge properties
             */
            final Map<String, Integer> edgeValuePropertyKeys = new HashMap<String, Integer>();
            /**
             * HashSet of configured edge labels
             */
            final Map<String, Integer> edgeLabelKeys = new HashMap<String, Integer>();


            enableVectorValue = VECTOR_VALUE.get(conf);
            vertexValuePropertyKeyList = INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.get(conf).split(regexp);
            edgeValuePropertyKeyList = INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.get(conf).split(regexp);
            edgeLabelList = INPUT_EDGE_LABEL_LIST.get(conf).split(regexp);
            vertexTypePropertyKey = VERTEX_TYPE_PROPERTY_KEY.get(conf);
            edgeTypePropertyKey = EDGE_TYPE_PROPERTY_KEY.get(conf);
            int size = vertexValuePropertyKeyList.length;
            for (int i = 0; i < size; i++) {
                vertexValuePropertyKeys.put(vertexValuePropertyKeyList[i], i);
            }
            for (int i = 0; i < edgeValuePropertyKeyList.length; i++) {
                edgeValuePropertyKeys.put(edgeValuePropertyKeyList[i], i);
            }

            for (int i = 0; i < edgeLabelList.length; i++) {
                edgeLabelKeys.put(edgeLabelList[i], i);
            }


            // set up vertex Value
            Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> vertex =  conf.createVertex();
            double[] data = new double[size];
            Vector vector = new DenseVector(data);
            vertexValueVector = new VertexData4CGDWritable(vertexType, vector.clone(), vector.clone(), vector.clone());
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), vertexValueVector);


            // Add check that property list contains single value
            for (final String propertyKey : vertexValuePropertyKeys.keySet()) {
                final Object vertexValueObject = faunusVertex.getProperty(propertyKey);
                vertexType = vertex.getValue().getType();
                Vector vector1 = vertex.getValue().getVector();
                if (enableVectorValue.equals("true")) {
                    //one property key has a vector as value
                    //split by either space or comma or tab
                    String[] valueString = vertexValueObject.toString().split(regexp);
                    for (int i = 0; i < valueString.length; i++) {
                        vector.set(i, Double.parseDouble(valueString[i]));
                    }
                } else {
                    final double vertexValue = Double.parseDouble(vertexValueObject.toString());
                    vector.set(vertexValuePropertyKeys.get(propertyKey), vertexValue);
                }
                vertex.setValue(new VertexData4CGDWritable(vertexType, vector, vector.clone(), vector.clone()));
            }

            final Object vertexTypeObject = faunusVertex.getProperty(vertexTypePropertyKey);
            if (vertexTypeObject != null) {
                Vector priorVector = vertex.getValue().getVector();
                String vertexTypeString = vertexTypeObject.toString().toLowerCase();
                if (vertexTypeString.equals(VERTEX_TYPE_LEFT)) {
                    vertexType = VertexData4CFWritable.VertexType.LEFT;
                } else if (vertexTypeString.equals(VERTEX_TYPE_RIGHT)) {
                    vertexType = VertexData4CFWritable.VertexType.RIGHT;
                } else {
                    LOG.error("Vertex type string: %s isn't supported." + vertexTypeString);
                    throw new IllegalArgumentException(String.format(
                            "Vertex type string: %s isn't supported.", vertexTypeString));
                }
                vertex.setValue(new VertexData4CGDWritable(vertexType, priorVector,
                        priorVector.clone(), priorVector.clone()));
            }

            for (final String edgeLabelKey : edgeLabelKeys.keySet()) {
                EdgeLabel edgeLabel = faunusVertex.tx().getEdgeLabel(edgeLabelKey);
                for (final TitanEdge titanEdge : faunusVertex.getTitanEdges(Direction.OUT, edgeLabel)) {
                    double edgeValue = 1.0d;
                    String edgeTypeString = null;
                    for (final String propertyKey : edgeValuePropertyKeys.keySet()) {
                        final Object edgeValueObject = titanEdge.getProperty(propertyKey);
                        edgeValue = Double.parseDouble(edgeValueObject.toString()); //TODO: ADD check

                        final Object edgeTypeObject = titanEdge.getProperty(edgeTypePropertyKey);
                        edgeTypeString = edgeTypeObject.toString().toLowerCase();
                        if (edgeTypeString.equals(TYPE_TRAIN)) {
                            edgeType = EdgeData4CFWritable.EdgeType.TRAIN;
                        } else if (edgeTypeString.equals(TYPE_VALIDATE)) {
                            edgeType = EdgeData4CFWritable.EdgeType.VALIDATE;
                        } else if (edgeTypeString.equals(TYPE_TEST)) {
                            edgeType = EdgeData4CFWritable.EdgeType.TEST;
                        } else {
                            LOG.error("Edge type string: %s isn't supported." + edgeTypeString);
                            throw new IllegalArgumentException(String.format(
                                    "Edge type string: %s isn't supported.", edgeTypeString));
                        }


                        Edge<LongWritable, EdgeData4CFWritable> edge = EdgeFactory.create(
                                new LongWritable(titanEdge.getOtherVertex(faunusVertex).getLongId()), new EdgeData4CFWritable(
                                        edgeType, edgeValue));
                        vertex.addEdge(edge);

                    }
                }
            }
            return (vertex);
        }
        /**
         * get current vertex with ID in long; value as two vectors, both in
         * double edge as two vectors, both in double
         *
         * @return Vertex Giraph vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Vertex<LongWritable, VertexData4CGDWritable, EdgeData4CFWritable> getCurrentVertex()
            throws IOException, InterruptedException {
            return vertex;
        }

        /**
         * get vertex value
         *
         * @return VertexData4CGDWritable vertex value in vector
         * @throws IOException
         */
        protected VertexData4CGDWritable getValue() throws IOException {
            VertexData4CGDWritable vertexValue = vertex.getValue();
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
         * get edges of this vertex
         *
         * @return Iterable of Giraph edges
         * @throws IOException
         */
        protected Iterable<Edge<LongWritable, EdgeData4CFWritable>> getEdges() throws IOException {
            return vertex.getEdges();
        }


        /**
         * close
         *
         * @throws IOException
         */
        public void close() throws IOException {
            this.graphReader.shutdown();
            super.close();
        }
    }
}
