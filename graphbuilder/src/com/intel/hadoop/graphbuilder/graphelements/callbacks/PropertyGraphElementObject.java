package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public class PropertyGraphElementObject implements PropertyGraphElementType {
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
}
