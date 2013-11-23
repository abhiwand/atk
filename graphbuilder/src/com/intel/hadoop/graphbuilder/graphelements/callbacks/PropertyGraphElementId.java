package com.intel.hadoop.graphbuilder.graphelements.callbacks;


import com.intel.hadoop.graphbuilder.graphelements.*;

public class PropertyGraphElementId implements PropertyGraphElementType {
    @Override
    public EdgeID edge(PropertyGraphElement propertyGraphElement) {
        Edge edge = (Edge)propertyGraphElement.get();
        return new EdgeID(edge.getSrc(), edge.getDst(), edge.getEdgeLabel());
    }

    @Override
    public Object vertex(PropertyGraphElement propertyGraphElement) {
        Vertex vertex = (Vertex)propertyGraphElement.get();
        return vertex.getVertexId();
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement) {
        return null;
    }
}
