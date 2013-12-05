package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.WritableComparable;

public class PropertyGraphElementLabel implements PropertyGraphElementTypeCallback {
    @Override
    public StringType edge(PropertyGraphElement propertyGraphElement, Object... args) {
        Edge edge = (Edge)propertyGraphElement;
        return edge.getEdgeLabel();
    }

    @Override
    public WritableComparable vertex(PropertyGraphElement propertyGraphElement, Object... args) {
        Vertex vertex = (Vertex)propertyGraphElement;
        return vertex.getVertexId();
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, Object... args) {
        return null;
    }
}
