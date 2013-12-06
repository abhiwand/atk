package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

/**
 * get the graph elements dst. if it's an edge it will return the dst otherwise it will return null
 *
 * @see PropertyGraphElement
 */
public class PropertyGraphElementDst implements PropertyGraphElementTypeCallback{
    @Override
    public Object edge(PropertyGraphElement propertyGraphElement, Object... args) {
        Edge edge = (Edge)propertyGraphElement;
        return edge.getDst();
    }

    @Override
    public Object vertex(PropertyGraphElement propertyGraphElement, Object... args) {
        return null;
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, Object... args) {
        return null;
    }
}
