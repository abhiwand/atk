package com.intel.hadoop.graphbuilder.graphelements.callbacks;


import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.PropertyMap;

public class PropertyGraphElementPropertyMap implements PropertyGraphElementType{
    @Override
    public PropertyMap edge(PropertyGraphElement propertyGraphElement, Object ... args) {
        return ((Edge)propertyGraphElement.get()).getProperties();
    }

    @Override
    public PropertyMap vertex(PropertyGraphElement propertyGraphElement, Object ... args) {
        return ((Vertex)propertyGraphElement.get()).getProperties();
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, Object ... args) {
        return null;
    }
}
