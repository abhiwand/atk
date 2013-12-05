package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public class PropertyGraphElementProperty implements PropertyGraphElementTypeCallback{
    private String key;

    @Override
    public Object edge(PropertyGraphElement propertyGraphElement, Object... args) {
        initArguments(args);

        Edge edge = (Edge)propertyGraphElement;
        return edge.getProperty(key);
    }

    @Override
    public Object vertex(PropertyGraphElement propertyGraphElement, Object... args) {
        initArguments(args);

        Vertex vertex = (Vertex)propertyGraphElement;
        return vertex.getProperty(key);
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, Object... args) {
        return null;
    }

    private void initArguments(Object ... args){
        key = (String) args[0];
    }
}
