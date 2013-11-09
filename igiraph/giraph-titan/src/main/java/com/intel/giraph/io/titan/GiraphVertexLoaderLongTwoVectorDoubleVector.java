//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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
package com.intel.giraph.io.titan;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_VERTEX_PROPERTY_KEY_LIST;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;

import com.intel.mahout.math.TwoVectorWritable;
import com.intel.mahout.math.DoubleWithVectorWritable;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.google.common.base.Preconditions;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * VertexInputFormat that features <code>long</code> vertex ID's,
 * <code>TwoVector</code> vertex values: one for prior and
 * one for posterior, and <code>DoubleVector</code> edge
 * weights.
 */
public class GiraphVertexLoaderLongTwoVectorDoubleVector {
    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(GiraphVertexLoaderLongTwoVectorDoubleVector.class);
    /**
     * whether it is Titan system type
     */
    private boolean isSystemType = false;
    /**
     * Long type vertex id
     */
    private long vertexId = 0;
    /**
     * Giraph Vertex
     */
    private Vertex<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> vertex = null;
    /**
     * HashMap of configured vertex properties
     */
    private final Map<String, Integer> vertexPropertyKeyValues = new HashMap<String, Integer>();
    /**
     * HashMap of configured edge properties
     */
    private final Map<String, Integer> edgePropertyKeyValues = new HashMap<String, Integer>();
    /**
     * HashSet of configured edge labels
     */
    private final Map<String, Integer> edgeLabelValues = new HashMap<String, Integer>();
    /**
     * vertex value vector
     */
    private TwoVectorWritable vertexValueVector = null;
    /**
     * Data vector
     */
    private Vector vector = null;


    /**
     * GiraphVertexLoaderLongTwoVectorDoubleVector Constructor with ID
     *
     * @param conf Giraph configuration
     * @param id   vertex id
     */
    public GiraphVertexLoaderLongTwoVectorDoubleVector(final ImmutableClassesGiraphConfiguration conf,
                                                       final long id) {
        /**
         * Vertex properties to filter
         */
        final String[] vertexPropertyKeyList;
        /**
         * Edge properties to filter
         */
        final String[] edgePropertyKeyList;
        /**
         * Edge labels to filter
         */
        final String[] edgeLabelList;

        vertexPropertyKeyList = INPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).split(",");
        edgePropertyKeyList = INPUT_EDGE_PROPERTY_KEY_LIST.get(conf).split(",");
        edgeLabelList = INPUT_EDGE_LABEL_LIST.get(conf).split(",");
        int size = vertexPropertyKeyList.length;
        for (int i = 0; i < size; i++) {
            vertexPropertyKeyValues.put(vertexPropertyKeyList[i], i);
        }

        for (int i = 0; i < edgePropertyKeyList.length; i++) {
            edgePropertyKeyValues.put(edgePropertyKeyList[i], i);
        }

        for (int i = 0; i < edgeLabelList.length; i++) {
            edgeLabelValues.put(edgeLabelList[i], i);
        }


        // set up vertex Value
        vertex = conf.createVertex();
        double[] data = new double[size];
        vector = new DenseVector(data);
        vertexValueVector = new TwoVectorWritable(vector.clone(), vector.clone());
        vertex.initialize(new LongWritable(id), vertexValueVector);
        vertexId = id;

    }

    /**
     * getVertex
     *
     * @return Giraph Vertex
     */
    public Vertex getVertex() {
        return this.isSystemType ? null : this.vertex;
    }

    /**
     * getFactory
     *
     * @return RelationFactory
     */
    public RelationFactory getFactory() {
        return new RelationFactory();
    }

    /**
     * Implement com.thinkaurelius.titan.graphdb.database.RelationFactory to
     * parse graph semantics on data loaded from HBase
     */
    public class RelationFactory implements com.thinkaurelius.titan.graphdb.database.RelationFactory {

        /**
         * Titan property
         */
        private final Map<String, Object> properties = new HashMap<String, Object>();
        /**
         * Relation Direction
         */
        private Direction direction;
        /**
         * Titan Type
         */
        private TitanType type;
        /**
         * Relation ID
         */
        private long relationID;
        /**
         * The other vertex ID for a Titan Edge
         */
        private long otherVertexID;
        /**
         * Property value
         */
        private Object value;

        /**
         * get vertex ID
         *
         * @return Long
         */
        @Override
        public long getVertexID() {
            return vertexId;
        }

        /**
         * set relation direction
         *
         * @param direction : Titan relation direction
         */
        @Override
        public void setDirection(final Direction direction) {
            this.direction = direction;
        }

        /**
         * set relation type
         *
         * @param type : Titan Type
         */
        @Override
        public void setType(final TitanType type) {
            if (type == SystemKey.TypeClass) {
                isSystemType = true;
            }
            this.type = type;
        }

        /**
         * set relation ID
         *
         * @param relationID titan relation id
         */
        @Override
        public void setRelationID(final long relationID) {
            this.relationID = relationID;
        }

        /**
         * set other vertex ID
         *
         * @param vertexId titan vertex id
         */
        @Override
        public void setOtherVertexID(final long vertexId) {
            if (vertexId < 0) {
                LOG.error("negative vertexId");
            }
            this.otherVertexID = vertexId;
        }

        /**
         * set value
         *
         * @param value :property value
         */
        @Override
        public void setValue(final Object value) {
            this.value = value;
        }

        /**
         * add relation property
         *
         * @param type  : TitanType
         * @param value : Titan property value
         */
        @Override
        public void addProperty(final TitanType type, final Object value) {
            properties.put(type.getName(), value);
        }

        /**
         * build Giraph Vertex and Edge
         */
        public void build() {
            if (this.type instanceof SystemType) {
                return;
            }
            if (this.type.isPropertyKey()) {
                Preconditions.checkNotNull(value);
                // filter vertex property key name
                String propertyName = this.type.getName();
                if (vertexPropertyKeyValues.containsKey(propertyName)) {
                    final Object vertexValueObject = this.value;
                    final double vertexValue = Double.parseDouble(vertexValueObject.toString());
                    Vector priorVector = vertexValueVector.getPriorVector();
                    priorVector.set(vertexPropertyKeyValues.get(propertyName), vertexValue);
                    vertex.setValue(new TwoVectorWritable(priorVector, vector.clone()));
                }
            } else {
                Preconditions.checkArgument(this.type.isEdgeLabel());
                // filter Edge Label
                if (this.relationID > 0) {
                    if (edgeLabelValues.containsKey(this.type.getName())) {
                        double edgeValue = 0.0d;
                        if (this.direction.equals(Direction.OUT)) {
                            for (final Map.Entry<String, Object> entry : this.properties.entrySet()) {
                                if (entry.getValue() != null &&
                                        edgePropertyKeyValues.containsKey(entry.getKey())) {
                                    final Object edgeValueObject = entry.getValue();
                                    edgeValue = Double.parseDouble(edgeValueObject.toString());
                                }
                            }
                            Edge<LongWritable, DoubleWithVectorWritable> edge = EdgeFactory.create(
                                    new LongWritable(this.otherVertexID), new DoubleWithVectorWritable(
                                        edgeValue, vector.clone()));
                            vertex.addEdge(edge);
                        } else if (this.direction.equals(Direction.BOTH)) {
                            throw ExceptionFactory.bothIsNotSupported();
                        }
                    }
                } else {
                    LOG.error("negative Edge ID.");
                }

            }
        }
    }

}
