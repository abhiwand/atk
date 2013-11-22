package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public class GraphElement implements GraphElementTypeCallback {
    private GraphElementId graphElementId;
    public GraphElement() {
        graphElementId = new GraphElementId();
    }

    @Override
    public Edge edge(PropertyGraphElement propertyGraphElement) {
        return propertyGraphElement.edge();
    }

    @Override
    public Vertex vertex(PropertyGraphElement propertyGraphElement) {
        return propertyGraphElement.vertex();
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement) {
        return null;
    }

    public <T> T getCallback(PropertyGraphElement propertyGraphElement, GraphElementTypeCallback graphElementTypeSwitchCallback){
        if(isEdge(propertyGraphElement)){
            return graphElementTypeSwitchCallback.edge(propertyGraphElement);
        }else if(isVertex(propertyGraphElement)){
            return graphElementTypeSwitchCallback.vertex(propertyGraphElement);
        }
        return null;
    }

    public Object getId(PropertyGraphElement propertyGraphElement){
        return this.getCallback(propertyGraphElement, this.graphElementId);
    }

    public Object getType(PropertyGraphElement propertyGraphElement){
        return this.getCallback(propertyGraphElement, this);
    }

    public boolean isEdge(PropertyGraphElement propertyGraphElement){
        return isGraphElementType(propertyGraphElement, PropertyGraphElement.GraphElementType.EDGE);
    }

    public boolean isVertex(PropertyGraphElement propertyGraphElement){
        return isGraphElementType(propertyGraphElement, PropertyGraphElement.GraphElementType.VERTEX);
    }

    public boolean isNull(PropertyGraphElement propertyGraphElement){
        return isGraphElementType(propertyGraphElement, PropertyGraphElement.GraphElementType.NULL_ELEMENT);
    }

    private boolean isGraphElementType(PropertyGraphElement propertyGraphElement, PropertyGraphElement.GraphElementType graphElementType){
        if(propertyGraphElement.graphElementType() == graphElementType){
            return true;
        }else{
            return false;
        }

    }
}
