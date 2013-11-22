package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;

public class GraphElement {
    private GraphElementId graphElementId;
    private GraphElementObject graphElementObject;
    public GraphElement() {
        graphElementObject = new GraphElementObject();
        graphElementId = new GraphElementId();
    }

    public <T> T typeCallback(PropertyGraphElement propertyGraphElement, GraphElementTypeCallback graphElementTypeSwitchCallback){
        if(isEdge(propertyGraphElement)){
            return graphElementTypeSwitchCallback.edge(propertyGraphElement);
        }else if(isVertex(propertyGraphElement)){
            return graphElementTypeSwitchCallback.vertex(propertyGraphElement);
        }
        return null;
    }

    public Object getId(PropertyGraphElement propertyGraphElement){
        return this.typeCallback(propertyGraphElement, this.graphElementId);
    }

    public Object get(PropertyGraphElement propertyGraphElement){
        return this.typeCallback(propertyGraphElement, this.graphElementObject);
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
