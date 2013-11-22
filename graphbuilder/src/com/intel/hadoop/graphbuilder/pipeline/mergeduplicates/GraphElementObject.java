package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public class GraphElementObject implements GraphElementTypeCallback{

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
