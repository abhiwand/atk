package com.intel.hadoop.graphbuilder.graphelements.callbacks;


import com.intel.hadoop.graphbuilder.graphelements.*;
import org.apache.hadoop.io.WritableComparable;

/**
 * get the graph elements id
 *
 * @see PropertyGraphElement
 */
public class PropertyGraphElementId implements PropertyGraphElementTypeCallback {
    @Override
    public EdgeID edge(PropertyGraphElement propertyGraphElement, Object ... args) {
        Edge edge = (Edge)propertyGraphElement;
        return edge.getEdgeID();//EdgeID(edge.getSrc(), edge.getDst(), edge.getEdgeLabel());
    }

    @Override
    public WritableComparable vertex(PropertyGraphElement propertyGraphElement, Object ... args) {
        Vertex vertex = (Vertex)propertyGraphElement;
        return vertex.getVertexId();
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, Object ... args) {
        return null;
    }
}
