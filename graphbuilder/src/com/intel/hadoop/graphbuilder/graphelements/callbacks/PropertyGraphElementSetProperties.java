package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.PropertyMap;

public class PropertyGraphElementSetProperties implements PropertyGraphElementTypeCallback {
    private PropertyMap propertyMap;

    @Override
    public <T> T edge(PropertyGraphElement propertyGraphElement, Object... args) {
        initArguments(args);

        Edge edge = (Edge)propertyGraphElement;
        edge.setProperties(propertyMap);
        return null;
    }

    @Override
    public <T> T vertex(PropertyGraphElement propertyGraphElement, Object... args) {
        initArguments(args);

        Vertex vertex = (Vertex)propertyGraphElement;
        vertex.setProperties(propertyMap);
        return null;
    }

    @Override
    public <T> T nullElement(PropertyGraphElement propertyGraphElement, Object... args) {
        return null;
    }

    private void initArguments(Object ... args){
        if(args.length == 1){
            propertyMap = (PropertyMap) args[0];
        }else{
            throw new IllegalArgumentException("Incorrect number of arguments expect exactly 1 given: " + args.length);
        }
    }
}
