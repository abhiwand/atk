package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.PropertyGraphElement;

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.callbacks.PropertyGraphElementType;
import org.apache.hadoop.io.Writable;

import java.util.Date;
import java.util.HashMap;

public class ContainsKey implements PropertyGraphElementType {
    private HashMap<EdgeID, Writable>   edgeSet;
    private HashMap<Object, Writable>   vertexSet;
    /*private class Arguments {
        HashMap<EdgeID, Writable>   edgeSet;
        HashMap<Object, Writable>   vertexSet;

        Arguments(HashMap<EdgeID, Writable> edgeSet, HashMap<Object, Writable> vertexSet){
            this.edgeSet = edgeSet;
            this.vertexSet = vertexSet;
        }
    }*/

    //private Arguments arguments;

    @Override
    public Boolean edge(PropertyGraphElement propertyGraphElement, Object ... args) {
        this.arguments(args);
        return this.edgeSet.containsKey(propertyGraphElement.getId());
    }

    @Override
    public Boolean vertex(PropertyGraphElement propertyGraphElement, Object ... args) {
        this.arguments(args);
        return this.vertexSet.containsKey(propertyGraphElement.getId());
    }

    @Override
    public Boolean nullElement(PropertyGraphElement propertyGraphElement, Object ... args) {
        return false;
    }

    private void arguments(Object ... args){
        this.edgeSet = (HashMap<EdgeID, Writable>)args[0];
        this.vertexSet = (HashMap<Object, Writable>)args[1];
    }
}
