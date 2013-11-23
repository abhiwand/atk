package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;

public interface PropertyGraphElementType {
    public <T> T edge(PropertyGraphElement propertyGraphElement);
    public <T> T vertex(PropertyGraphElement propertyGraphElement);
    public <T> T nullElement(PropertyGraphElement propertyGraphElement);
}
