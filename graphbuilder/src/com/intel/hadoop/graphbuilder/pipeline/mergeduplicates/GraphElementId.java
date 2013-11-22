package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public class GraphElementId implements GraphElementTypeCallback {

    private GraphElement graphElement;

    public GraphElementId() {
        this.graphElement = new GraphElement();
    }

    @Override
    public EdgeID edge(PropertyGraphElement propertyGraphElement) {
        Edge edge = (Edge)graphElement.getType(propertyGraphElement);
        return new EdgeID(edge.getSrc(), edge.getDst(), edge.getEdgeLabel());
    }

    @Override
    public Object vertex(PropertyGraphElement propertyGraphElement) {
        Vertex vertex = (Vertex)graphElement.getType(propertyGraphElement);
        return vertex.getVertexId();
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement) {
        return null;
    }
}
