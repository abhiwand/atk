package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;

/**
 * Very simple interface that gets called when the property graph element is an edge, vertex or null graph element.
 * This essentially allows us to do a callback based on graph element type and it centralizes the branching on type to a
 * single function to PropertyGraphElement.typeCallback(). Makes the PropertyGraphElement much nicer because you don't
 * have to worry about weather you vertex or an edge has much.
 *
 * <P><b>With interface</b><br />
 * SerializedPropertyGraphElement.graphElement().getType()
 * <b>without interface</b>
 * ((Edge)SerializedPropertyGraphElement.graphElement()).getType()
 * </p>
 *
 * <b>For a sample usage look at</b>
 * @see PropertyGraphElement
 * @see PropertyGraphElementId
 * @see PropertyGraphElementObject
 */
public interface PropertyGraphElementTypeCallback {
    public <T> T edge(PropertyGraphElement propertyGraphElement, Object ... args);
    public <T> T vertex(PropertyGraphElement propertyGraphElement, Object ... args);
    public <T> T nullElement(PropertyGraphElement propertyGraphElement, Object ... args);
}
