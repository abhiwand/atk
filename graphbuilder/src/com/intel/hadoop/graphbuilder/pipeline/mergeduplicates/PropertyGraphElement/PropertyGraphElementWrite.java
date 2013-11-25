package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.PropertyGraphElement;

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.callbacks.PropertyGraphElementType;
import com.intel.hadoop.graphbuilder.util.Functional;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PropertyGraphElementWrite implements PropertyGraphElementType{
    private HashMap<EdgeID, Writable> edgeSet;
    private HashMap<Object, Writable>   vertexSet;

    @Override
    public <T> T edge(PropertyGraphElement propertyGraphElement, Object... args) {
        this.arguments(args);

        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> T vertex(PropertyGraphElement propertyGraphElement, Object... args) {
        this.arguments(args);

        Iterator<Map.Entry<Object, Writable>> vertexIterator = vertexSet.entrySet().iterator();
        for(Map.Entry<Object, Writable> vertexProp: vertexSet.entrySet()){

        }
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> T nullElement(PropertyGraphElement propertyGraphElement, Object... args) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private void arguments(Object ... args){
        edgeSet = (HashMap<EdgeID, Writable>)args[0];
        vertexSet = (HashMap<Object, Writable>)args[1];
        /*edgeReducerFunction = (Functional)args[2];
        vertexReducerFunction = (Functional)args[3];
        noBiDir = (boolean)args[4];*/
    }
}
