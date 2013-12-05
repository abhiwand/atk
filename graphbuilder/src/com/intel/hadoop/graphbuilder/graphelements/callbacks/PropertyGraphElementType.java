package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;

public class PropertyGraphElementType implements PropertyGraphElementTypeCallback{
    public enum GraphType{EDGE,VERTEX,NULL}
    @Override
    public GraphType edge(PropertyGraphElement propertyGraphElement, Object... args) {
        return GraphType.EDGE;
    }

    @Override
    public GraphType vertex(PropertyGraphElement propertyGraphElement, Object... args) {
        return GraphType.VERTEX;
    }

    @Override
    public GraphType nullElement(PropertyGraphElement propertyGraphElement, Object... args) {
        return GraphType.NULL;
    }
}
