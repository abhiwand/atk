package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import org.apache.hadoop.io.WritableComparable;

/**
 * get the graph element src. if it's an edge it will return whatever src the edge has otherwise it will return null
 * if it's a vertex.
 *
 * @see PropertyGraphElement
 */
public class PropertyGraphElementSrc implements PropertyGraphElementTypeCallback {
    @Override
    public Object edge(PropertyGraphElement propertyGraphElement, Object... args) {
        Edge edge = (Edge)propertyGraphElement;
        return edge.getSrc();
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
