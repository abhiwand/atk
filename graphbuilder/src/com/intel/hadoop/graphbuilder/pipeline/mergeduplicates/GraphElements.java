package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;

import java.util.Iterator;

public class GraphElements {
   /* public boolean isEdge(PropertyGraphElement propertyGraphElement){
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

    }*/

   /* public <T> T graphElementTypeCallback(PropertyGraphElement propertyGraphElement, GraphElementTypeCallback graphElementTypeSwitch){
        if(isEdge(propertyGraphElement)){
            return graphElementTypeSwitch.edge(propertyGraphElement);
        }else if(isVertex(propertyGraphElement)){
            return graphElementTypeSwitch.vertex(propertyGraphElement);
        }
        return null;
    }*/

    public void mergeDuplicates(Iterable<PropertyGraphElement> values){
        //GraphElement graphElement = new GraphElement();
        Iterator<PropertyGraphElement> valueIterator = values.iterator();

        for(PropertyGraphElement propertyGraphElement: values){
            //null element check
            if(propertyGraphElement.isNull()){
                continue;
            }


            Object id = propertyGraphElement.getId();




        }
    }
}
