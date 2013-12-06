package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;

/**
 * return the type of the graph element. Very use full when you don't care if it's an edge or vertex you just want a
 * type
 *
 * @see PropertyGraphElement
 */
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
