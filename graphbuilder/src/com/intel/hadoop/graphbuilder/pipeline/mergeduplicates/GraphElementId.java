package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public class GraphElementId  {

    private GraphElementObject graphElement;

    public GraphElementId() {
        this.graphElement = new GraphElementObject();
    }

   /* @Override
    public EdgeID edge(PropertyGraphElement propertyGraphElement) {
        Edge edge = (Edge)graphElement.get(propertyGraphElement);
        return new EdgeID(edge.getSrc(), edge.getDst(), edge.getEdgeLabel());
    }

    @Override
    public Object vertex(PropertyGraphElement propertyGraphElement) {
        Vertex vertex = (Vertex)graphElement.get(propertyGraphElement);
        return vertex.getVertexId();
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement) {
        return null;
    }*/
}
