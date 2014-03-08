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

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.util.*;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;


/**
 * GiraphVertexLoaderLongDoubleNull loads vertex
 * with <code>Long</code> vertex ID's,
 * <code>Double</code> vertex values,
 * and <code>Float</code> edge weights.
 */
public class GiraphVertexLoaderLongDoubleNull {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(GiraphVertexLoaderLongDoubleNull.class);
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
    private Vertex<LongWritable, DoubleWritable, NullWritable> vertex = null;
    /**
     * HashSet of configured vertex properties
     */
    private Set<String> vertexValuePropertyKeys = null;
    /**
     * HashSet of configured edge properties
     */
    private Set<String> edgeValuePropertyKeys = null;
    /**
     * HashSet of configured edge labels
     */
    private Set<String> edgeLabelKeys = null;
    /**
     * regular expression of the deliminators for a property list
     */
    private String regexp = "[\\s,\\t]+";     //.split("/,?\s+/");

    /**
     * GiraphVertexLoaderLongDoubleNull constructor
     *
     * @param conf : Giraph configuration
     * @param id   vertex id
     */
    public GiraphVertexLoaderLongDoubleNull(final ImmutableClassesGiraphConfiguration conf, final long id) {
        /** Vertex value properties to filter */
        final String[] vertexValuePropertyKeyList;
        /** Edge value properties to filter */
        final String[] edgeValuePropertyKeyList;
        /** Edge labels to filter */
        final String[] edgeLabelList;

        vertex = conf.createVertex();
        vertex.initialize(new LongWritable(id), new DoubleWritable(0));
        vertexId = id;
        vertexValuePropertyKeyList = INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.get(conf).split(regexp);
        edgeValuePropertyKeyList = INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.get(conf).split(regexp);
        edgeLabelList = INPUT_EDGE_LABEL_LIST.get(conf).split(regexp);
        vertexValuePropertyKeys = new HashSet<String>(Arrays.asList(vertexValuePropertyKeyList));
        edgeValuePropertyKeys = new HashSet<String>(Arrays.asList(edgeValuePropertyKeyList));
        edgeLabelKeys = new HashSet<String>(Arrays.asList(edgeLabelList));
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
                LOG.error(INVALID_VERTEX_ID + vertexId);
                throw new RuntimeException(INVALID_VERTEX_ID + vertexId);
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
                if (vertexValuePropertyKeys.contains(this.type.getName())) {
                    final Object vertexValueObject = this.value;
                    final double vertexValue = Double.parseDouble(vertexValueObject.toString());
                    vertex.setValue(new DoubleWritable(vertexValue));
                }
            } else {
                Preconditions.checkArgument(this.type.isEdgeLabel());
                // filter Edge Label
                if (this.relationID > 0) {
                    if (edgeLabelKeys.contains(this.type.getName())) {
                        if (this.direction.equals(Direction.OUT)) {
                            Edge<LongWritable, NullWritable> edge = EdgeFactory.create(new LongWritable(
                                this.otherVertexID), NullWritable.get());
                            vertex.addEdge(edge);

                        } else if (this.direction.equals(Direction.BOTH)) {
                            throw ExceptionFactory.bothIsNotSupported();
                        }
                    }
                } else {
                    LOG.error(INVALID_EDGE_ID + this.relationID);
                    throw new RuntimeException(INVALID_EDGE_ID + this.relationID);
                }

            }
        }
    }
}
