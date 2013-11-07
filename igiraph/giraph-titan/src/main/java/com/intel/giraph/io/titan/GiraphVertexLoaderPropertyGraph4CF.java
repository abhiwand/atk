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

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.INPUT_EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.INPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.EDGE_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.VERTEX_TYPE_PROPERTY_KEY;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;

import com.intel.giraph.io.EdgeDataWritable;
import com.intel.giraph.io.EdgeDataWritable.EdgeType;
import com.intel.giraph.io.VertexDataWritable;
import com.intel.giraph.io.VertexDataWritable.VertexType;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.google.common.base.Preconditions;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Vertex Loader to read vertex from Titan.
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
 * [1,[4,3],[l],[[2,2.1,[tr]],[3,0.7,[va]]]]
 */
public class GiraphVertexLoaderPropertyGraph4CF {
    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(GiraphVertexLoaderPropertyGraph4CF.class);
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
    private Vertex<LongWritable, VertexDataWritable, EdgeDataWritable> vertex = null;
    /**
     * Property key for Vertex Type
     */
    private final String vertexTypePropertyKey;
    /**
     * Property key for Edge Type
     */
    private final String edgeTypePropertyKey;
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
     * vertex value
     */
    private VertexDataWritable vertexValueVector = null;
    /**
     * the vertex type
     */
    private VertexType vertexType = VertexType.NONE;
    /**
     * the edge type
     */
    private EdgeType edgeType = EdgeType.NONE;


    /**
     * GiraphVertexLoaderPropertyGraph4CF Constructor with ID
     *
     * @param conf Giraph configuration
     * @param id   vertex id
     */
    public GiraphVertexLoaderPropertyGraph4CF(final ImmutableClassesGiraphConfiguration conf,
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
        vertexTypePropertyKey = VERTEX_TYPE_PROPERTY_KEY.get(conf);
        edgeTypePropertyKey = EDGE_TYPE_PROPERTY_KEY.get(conf);
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
        Vector vector = new DenseVector(data);
        vertexValueVector = new VertexDataWritable(vertexType, vector.clone());
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
                    Vector vector = vertexValueVector.getVector();
                    vector.set(vertexPropertyKeyValues.get(propertyName), vertexValue);
                    vertex.setValue(new VertexDataWritable(vertexType, vector));
                }
                //get the vertex type
                else if (propertyName.equals(vertexTypePropertyKey)) {
                    final Object vertexTypeObject = this.value;
                    Vector priorVector = vertexValueVector.getVector();

                    String vertexTypeString = vertexTypeObject.toString();
                    if (vertexTypeString.equals("l")) {
                        vertexType = VertexType.LEFT;
                    } else if (vertexTypeString.equals("r")) {
                        vertexType = VertexType.RIGHT;
                    } else {
                        LOG.error("Vertex type string: %s isn't supported." + vertexTypeString);
                        throw new IllegalArgumentException(String.format(
                                "Vertex type string: %s isn't supported.", vertexTypeString));
                    }
                    vertex.setValue(new VertexDataWritable(vertexType, priorVector));
                }
            } else {
                Preconditions.checkArgument(this.type.isEdgeLabel());
                // filter Edge Label
                if (this.relationID > 0) {
                    if (edgeLabelValues.containsKey(this.type.getName())) {
                        double edgeValue = 0.0d;
                        if (this.direction.equals(Direction.OUT)) {
                            String edgeTypeString = null;
                            for (final Map.Entry<String, Object> entry : this.properties.entrySet()) {
                                Preconditions.checkNotNull(entry.getValue());
                                if (edgePropertyKeyValues.containsKey(entry.getKey())) {
                                    final Object edgeValueObject = entry.getValue();
                                    edgeValue = Double.parseDouble(edgeValueObject.toString());
                                }
                                //get the edge type
                                else if (edgeTypePropertyKey.equals(entry.getKey())) {
                                    final Object edgeTypeObject = entry.getValue();
                                    edgeTypeString = edgeTypeObject.toString();
                                    if (edgeTypeString.equals("tr")) {
                                        edgeType = EdgeType.TRAIN;
                                    } else if (edgeTypeString.equals("va")) {
                                        edgeType = EdgeType.VALIDATE;
                                    } else if (edgeTypeString.equals("te")) {
                                        edgeType = EdgeType.TEST;
                                    } else {
                                        LOG.error("Edge type string: %s isn't supported." + edgeTypeString);
                                        throw new IllegalArgumentException(String.format(
                                                "Edge type string: %s isn't supported.", edgeTypeString));
                                    }
                                }
                            }
                            Edge<LongWritable, EdgeDataWritable> edge = EdgeFactory.create(
                                    new LongWritable(this.otherVertexID), new EdgeDataWritable(
                                    edgeType, edgeValue));
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
