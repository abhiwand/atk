package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public class PropertyGraphElementObject implements PropertyGraphElementTypeCallback {

    @Override
    public Edge edge(PropertyGraphElement propertyGraphElement, Object ... args) {
        return (Edge)propertyGraphElement;
    }

    @Override
    public Vertex vertex(PropertyGraphElement propertyGraphElement, Object ... args) {
        return (Vertex)propertyGraphElement;
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, Object ... args) {
        return null;
    }
}
