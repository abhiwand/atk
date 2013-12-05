package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created with IntelliJ IDEA.
 * User: rodorad
 * Date: 12/5/13
 * Time: 10:31 AM
 * To change this template use File | Settings | File Templates.
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
