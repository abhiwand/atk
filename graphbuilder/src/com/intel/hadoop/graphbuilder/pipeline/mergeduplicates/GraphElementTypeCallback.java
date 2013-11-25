package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;


import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;

public interface GraphElementTypeCallback {
    public <T> T edge(PropertyGraphElement propertyGraphElement);
    public <T> T vertex(PropertyGraphElement propertyGraphElement);
    public <T> T nullElement(PropertyGraphElement propertyGraphElement);


}
