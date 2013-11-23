package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public class PropertyGraphElementToString implements PropertyGraphElementType {
    @Override
    public String edge(PropertyGraphElement propertyGraphElement) {
        return ((Edge)propertyGraphElement.get()).toString();
    }

    @Override
    public String vertex(PropertyGraphElement propertyGraphElement) {
        return ((Vertex)propertyGraphElement.get()).toString();
    }

    @Override
    public String nullElement(PropertyGraphElement propertyGraphElement) {
        return new String("null graph element");
    }
}
