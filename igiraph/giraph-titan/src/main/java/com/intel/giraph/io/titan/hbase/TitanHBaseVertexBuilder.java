package com.intel.giraph.io.titan.hbase;

import com.intel.giraph.io.EdgeData4CFWritable;
import com.intel.giraph.io.VertexData4CFWritable;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.math.*;

import java.util.*;
import java.util.Vector;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EDGE_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_PROPERTY_KEY;

public class TitanHBaseVertexBuilder {

    /** Enable vector value */
    protected boolean enableVectorValue = false;

    /** HashMap of configured vertex properties */
    protected final Map<String, Integer> vertexValuePropertyKeys;

    /** HashMap of configured edge properties */
    protected final Map<String, Integer> edgeValuePropertyKeys;

    /** HashSet of configured edge labels */
    protected final Map<String, Integer> edgeLabelKeys;

    /** Property key for Vertex Type */
    protected String vertexTypePropertyKey;

    /** Property key for Edge Type */
    protected String edgeTypePropertyKey;

    /** Regex for splitting delimited strings */
    protected String regexp = "[\\s,\\t]+";

    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexBuilder.class);

    /**
     * Constructs a Titan/HBase vertex builder.
     *
     * Reads the Giraph configuration to extract the vertex and edge property names,
     * and edge labels needed by the builder.
     *
     * @param conf Giraph configuration file.
     */
    public TitanHBaseVertexBuilder(final ImmutableClassesGiraphConfiguration conf) {
        String[] vertexValuePropertyKeyList = INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.get(conf).split(regexp);
        String[] edgeValuePropertyKeyList = INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.get(conf).split(regexp);
        String[] edgeLabelList = INPUT_EDGE_LABEL_LIST.get(conf).split(regexp);

        vertexValuePropertyKeys = getConfigurationMap(vertexValuePropertyKeyList);
        edgeValuePropertyKeys = getConfigurationMap(edgeValuePropertyKeyList);
        edgeLabelKeys = getConfigurationMap(edgeLabelList);

        enableVectorValue = VECTOR_VALUE.get(conf).equals("true");
        vertexTypePropertyKey = VERTEX_TYPE_PROPERTY_KEY.get(conf);
        edgeTypePropertyKey = EDGE_TYPE_PROPERTY_KEY.get(conf);
    }

    /**
     * Constructs an iterator of the Titan vertex properties from a Faunus vertex.
     *
     * The vertex properties are filtered by property name.
     *
     * @param faunusVertex Titan/Hadoop (faunus) vertex
     * @return Titan vertex properties filtered by property name.
     */
    public Iterator<TitanProperty> buildTitanProperties(FaunusVertex faunusVertex) {
        ArrayList<TitanProperty> titanProperties = new ArrayList<>();

        if (vertexValuePropertyKeys!= null && vertexValuePropertyKeys.size() > 0) {
            String propertyKey = vertexValuePropertyKeys.keySet().iterator().next();
            Iterator<TitanProperty> propertyIterator = faunusVertex.getProperties(propertyKey).iterator();

            while (propertyIterator.hasNext()) {
                titanProperties.add(propertyIterator.next());
            }
        }
        return(titanProperties.iterator());
    }

    /**
     * Get CF vertex property type
     *
     * @param faunusVertex          Faunus (Titan/Hadoop) vertex
     * @param vertexTypePropertyKey Property name for vertex type
     * @return Vertex property type (Left or Right)
     * @throws IllegalArgumentException
     */
    public VertexData4CFWritable.VertexType getCFVertexTypeProperty(FaunusVertex faunusVertex,
                                                                     String vertexTypePropertyKey)
            throws IllegalArgumentException {
        VertexData4CFWritable.VertexType vertexType;
        Object vertexTypeObject = faunusVertex.getProperty(vertexTypePropertyKey);

        if (vertexTypeObject != null) {
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
        } else {
            LOG.error(String.format("Vertex type property %s should not be null", vertexTypePropertyKey));
            throw new IllegalArgumentException(String.format(
                    "Vertex type property %s should not be null", vertexTypePropertyKey));
        }
        return (vertexType);
    }

    /**
     * Get CF edge property type.
     *
     * @param titanEdge           Titan edge
     * @param edgeTypePropertyKey Edge type property name
     * @return Edge type
     */
    public EdgeData4CFWritable.EdgeType getCFEdgeTypeProperty(TitanEdge titanEdge, String edgeTypePropertyKey) {
        EdgeData4CFWritable.EdgeType edgeType;
        Object edgeTypeObject = titanEdge.getProperty(edgeTypePropertyKey);
        String edgeTypeString = edgeTypeObject.toString().toLowerCase();

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
        return edgeType;
    }

    /**
     * Create Giraph edge from Titan edge
     *
     * @param faunusVertex Faunus (Titan/Hadoop) vertex
     * @param titanEdge    Titan edge
     * @param edgeType     Edge Type
     * @param propertyKey  Edge property name used to set edge value
     * @return Giraph edge
     */
    public Edge<LongWritable, EdgeData4CFWritable> getCFGiraphEdge(FaunusVertex faunusVertex, TitanEdge titanEdge,
                                                                    EdgeData4CFWritable.EdgeType edgeType,
                                                                    String propertyKey) {
        Object edgeValueObject = titanEdge.getProperty(propertyKey);
        double edgeValue = 1.0d;

        try {
            edgeValue = Double.parseDouble(edgeValueObject.toString());
        } catch (NumberFormatException e) {
            LOG.warn("Unable to parse double value for property: " + edgeValueObject);
        }

        return EdgeFactory.create(
                new LongWritable(titanEdge.getOtherVertex(faunusVertex).getLongId()),
                new EdgeData4CFWritable(edgeType, edgeValue));
    }


    /**
     * Update vector values using Titan property value
     *
     * @param vector Mahout vector
     * @param titanProperty Titan property
     * @return Updated Mahout vector
     */
    public org.apache.mahout.math.Vector setVector(org.apache.mahout.math.Vector vector, TitanProperty titanProperty) {
        Object vertexValueObject = titanProperty.getValue();
        if (enableVectorValue) {
            //one property key has a vector as value
            //split by either space or comma or tab
            String[] valueString = vertexValueObject.toString().split(regexp);
            for (int i = 0; i < valueString.length; i++) {
                vector.set(i, Double.parseDouble(valueString[i]));
            }
        } else {
            String propertyName = titanProperty.getPropertyKey().getName();
            int propertyIndex = vertexValuePropertyKeys.get(propertyName);
            double vertexValue = Double.parseDouble(vertexValueObject.toString());
            vector.set(propertyIndex, vertexValue);
        }
        return vector;
    }

    /**
     * Construct Titan edges filtered by edge label
     *
     * @param faunusVertex Titan/Hadoop (faunus) vertex
     * @return Titan edges filtered by edge label
     */
    public Iterator<TitanEdge> buildTitanEdges(FaunusVertex faunusVertex) {

        if (edgeLabelKeys != null) {
            EdgeLabel[] edgeLabelList = getTitanEdgeLabels(faunusVertex);
            return(faunusVertex.getTitanEdges(Direction.OUT, edgeLabelList).iterator());
        }
        else {
            return(Collections.<TitanEdge>emptyList().iterator());
        }
    }

    /**
     * Get Titan edge labels using the edge labels specified in the Giraph configuration.
     *
     * @param faunusVertex Titan/Hadoop (faunus) vertex
     * @return Titan edge labels in the Giraph configuration
     */
    private EdgeLabel[] getTitanEdgeLabels(FaunusVertex faunusVertex) {
        ArrayList<EdgeLabel> edgeLabels = new ArrayList<>();
        for (final String edgeLabelKey : edgeLabelKeys.keySet()) {
            EdgeLabel edgeLabel = faunusVertex.tx().getEdgeLabel(edgeLabelKey);
            edgeLabels.add(edgeLabel);
        }
        return edgeLabels.toArray(new EdgeLabel[edgeLabels.size()]);
    }

    /**
     * Get a Hashmap with name of configuration parameter, and corresponding index in list.
     *
     * @param configList List of configuration parameters
     * @return Hashmap with name of configuration parameter, and corresponding index in list
     */
    private Map<String, Integer> getConfigurationMap(String[] configList) {
        Map<String, Integer> configMap = new HashMap<String, Integer>();
        for (int i = 0; i < configList.length; i++) {
            configMap.put(configList[i], i);
        }
        return (configMap);
    }
}
