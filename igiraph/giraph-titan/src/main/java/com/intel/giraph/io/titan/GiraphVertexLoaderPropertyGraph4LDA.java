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
package com.intel.giraph.io.titan;

import com.google.common.base.Preconditions;
import com.intel.giraph.io.VertexData4LDAWritable;
import com.intel.giraph.io.VertexData4LDAWritable.VertexType;
import com.intel.mahout.math.DoubleWithVectorWritable;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.util.HashMap;
import java.util.Map;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_VALUE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INVALID_EDGE_ID;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INVALID_VERTEX_ID;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VECTOR_VALUE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_LEFT;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_RIGHT;

/**
 * GiraphVertexLoaderPropertyGraph4LDA loads vertex from Titan.
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
public class GiraphVertexLoaderPropertyGraph4LDA {
    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(GiraphVertexLoaderPropertyGraph4LDA.class);
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
    private Vertex<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> vertex = null;
    /**
     * Property key for Vertex Type
     */
    private final String vertexTypePropertyKey;
    /**
     * HashMap of configured vertex properties
     */
    private final Map<String, Integer> vertexValuePropertyKeys = new HashMap<String, Integer>();
    /**
     * HashMap of configured edge properties
     */
    private final Map<String, Integer> edgeValuePropertyKeys = new HashMap<String, Integer>();
    /**
     * HashSet of configured edge labels
     */
    private final Map<String, Integer> edgeLabelKeys = new HashMap<String, Integer>();
    /**
     * vertex value
     */
    private VertexData4LDAWritable vertexValueVector = null;
    /**
     * the vertex type
     */
    private VertexType vertexType = null;
    /**
     * Enable vector value
     */
    private String enableVectorValue = "true";
    /**
     * regular expression of the deliminators for a property list
     */
    private String regexp = "[\\s,\\t]+";     //.split("/,?\s+/");

    /**
     * GiraphVertexLoaderPropertyGraph4LDA Constructor with ID
     *
     * @param conf Giraph configuration
     * @param id   vertex id
     */
    public GiraphVertexLoaderPropertyGraph4LDA(final ImmutableClassesGiraphConfiguration conf,
                                               final long id) {
        /**
         * Vertex value properties to filter
         */
        final String[] vertexValuePropertyKeyList;
        /**
         * Edge value properties to filter
         */
        final String[] edgeValuePropertyKeyList;
        /**
         * Edge labels to filter
         */
        final String[] edgeLabelList;

        enableVectorValue = VECTOR_VALUE.get(conf);
        vertexValuePropertyKeyList = INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.get(conf).split(regexp);
        edgeValuePropertyKeyList = INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.get(conf).split(regexp);
        edgeLabelList = INPUT_EDGE_LABEL_LIST.get(conf).split(regexp);
        vertexTypePropertyKey = VERTEX_TYPE_PROPERTY_KEY.get(conf);
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
        vertex = conf.createVertex();
        double[] data = new double[size];
        Vector vector = new DenseVector(data);
        vertexValueVector = new VertexData4LDAWritable(vertexType, vector.clone());
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
                String propertyName = this.type.getName();
                if (vertexValuePropertyKeys.containsKey(propertyName)) {
                    final Object vertexValueObject = this.value;
                    Vector vector = vertex.getValue().getVector();
                    vertexType = vertex.getValue().getType();
                    if (enableVectorValue.equals("true")) {
                        //one property key has a vector as value
                        //split by either space or comma or tab
                        String[] valueString = vertexValueObject.toString().split(regexp);
                        for (int i = 0; i < valueString.length; i++) {
                            vector.set(i, Double.parseDouble(valueString[i]));
                        }
                    } else {
                        final double vertexValue = Double.parseDouble(vertexValueObject.toString());
                        vector.set(vertexValuePropertyKeys.get(propertyName), vertexValue);
                    }
                    vertex.setValue(new VertexData4LDAWritable(vertexType, vector));
                } else if (propertyName.equals(vertexTypePropertyKey)) {
                    final Object vertexTypeObject = this.value;
                    Vector priorVector = vertex.getValue().getVector();

                    String vertexTypeString = vertexTypeObject.toString().toLowerCase();
                    if (vertexTypeString.equals(VERTEX_TYPE_LEFT)) {
                        vertexType = VertexType.LEFT;
                    } else if (vertexTypeString.equals(VERTEX_TYPE_RIGHT)) {
                        vertexType = VertexType.RIGHT;
                    } else {
                        LOG.error("Vertex type string: %s isn't supported." + vertexTypeString);
                        throw new IllegalArgumentException(String.format(
                            "Vertex type string: %s isn't supported.", vertexTypeString));
                    }
                    vertex.setValue(new VertexData4LDAWritable(vertexType, priorVector));
                }
            } else {
                Preconditions.checkArgument(this.type.isEdgeLabel());
                // filter Edge Label
                if (this.relationID > 0) {
                    if (edgeLabelKeys.containsKey(this.type.getName())) {
                        double edgeValue = 1.0d;
                        if (this.direction.equals(Direction.OUT)) {
                            for (final Map.Entry<String, Object> entry : this.properties.entrySet()) {
                                Preconditions.checkNotNull(entry.getValue());
                                if (edgeValuePropertyKeys.containsKey(entry.getKey())) {
                                    final Object edgeValueObject = entry.getValue();
                                    edgeValue = Double.parseDouble(edgeValueObject.toString());
                                }
                            }
                            Edge<LongWritable, DoubleWithVectorWritable> edge = EdgeFactory.create(
                                new LongWritable(this.otherVertexID), new DoubleWithVectorWritable(
                                    edgeValue, new DenseVector()));
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
