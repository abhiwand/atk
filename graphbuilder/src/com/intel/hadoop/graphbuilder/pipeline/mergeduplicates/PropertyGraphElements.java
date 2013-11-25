package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.Iterator;

public class PropertyGraphElements {
    HashMap<EdgeID, Writable> edgeSet;
    HashMap<Object, Writable> vertexSet;

    public PropertyGraphElements() {
        this.edgeSet = new HashMap<EdgeID, Writable>();
        this.vertexSet = new HashMap<Object, Writable>();
    }

    public void mergeDuplicates(Iterable<PropertyGraphElement> values){
        //GraphElement graphElement = new GraphElement();
        Iterator<PropertyGraphElement> valueIterator = values.iterator();

        for(PropertyGraphElement propertyGraphElement: values){
            //null element check
            if(propertyGraphElement.isNull()){
                continue;
            }


            Object id = propertyGraphElement.getId();


            System.out.println(id);

        }
    }
}
