package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;

import java.util.Iterator;

public class GraphElements {
    /*private final List<MyEventListener> listeners;

    *//*public GraphElements() {
        listeners = new CopyOnWriteArrayList<MyEventListener>();
    }*/

    public Object getGraphElementId(PropertyGraphElement propertyGraphElement){
        GraphElementTypeCallback id = new GraphElementId();
        return graphElementTypeCallback(propertyGraphElement, id);

    }

    /*public void addMyEventListener(MyEventListener listener) {
        listeners.add(listener);
    }
    public void removeMyEventListener(MyEventListener listener) {
        listeners.remove(listener);
    }
    void fireEvent() {
        MyEvent event = new MyEvent(this);
        for (MyEventListener listener : listeners) {
            listener.handleEvent(event);
        }
    }*/

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

    public <T> T graphElementTypeCallback(PropertyGraphElement propertyGraphElement, GraphElementTypeCallback graphElementTypeSwitch){
        if(isEdge(propertyGraphElement)){
            return graphElementTypeSwitch.edge(propertyGraphElement);
        }else if(isVertex(propertyGraphElement)){
            return graphElementTypeSwitch.vertex(propertyGraphElement);
        }
        return null;
    }

    public void mergeDuplicates(Iterable<PropertyGraphElement> values){
        GraphElement graphElement = new GraphElement();
        Iterator<PropertyGraphElement> valueIterator = values.iterator();

        for(PropertyGraphElement propertyGraphElement: values){
            //null element check
            if(isNull(propertyGraphElement)){
                continue;
            }


            Object id = graphElement.getId(propertyGraphElement);



            if(isVertex(propertyGraphElement)){

            }else if(isEdge(propertyGraphElement)){

            }
        }
    }
}
