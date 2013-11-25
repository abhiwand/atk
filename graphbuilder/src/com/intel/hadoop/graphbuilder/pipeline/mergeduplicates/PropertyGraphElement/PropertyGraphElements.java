package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.PropertyGraphElement;

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.util.Functional;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Iterator;

public class PropertyGraphElements {
    private ContainsKey containsKey;
    private PropertyGraphElementPut propertyGraphElementPut;

    private Reducer.Context context;
    private boolean noBiDir;
    private Functional edgeReducerFunction;

    private Functional vertexReducerFunction;

    HashMap<EdgeID, Writable>   edgeSet       = new HashMap<>();
    HashMap<Object, Writable>   vertexSet     = new HashMap<>();


    public PropertyGraphElements(){
        containsKey = new ContainsKey();
        propertyGraphElementPut = new PropertyGraphElementPut();
    }

    public PropertyGraphElements(Functional vertexReducerFunction, Functional edgeReducerFunction, Reducer.Context context){
        containsKey = new ContainsKey();
        this.vertexReducerFunction = vertexReducerFunction;
        this.edgeReducerFunction = edgeReducerFunction;
        this.context = context;
        this.noBiDir = context.getConfiguration().getBoolean("noBiDir", false);
    }

    public void mergeDuplicates(Iterable<com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement> values){
        Iterator<com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement> valueIterator = values.iterator();

        for(PropertyGraphElement propertyGraphElement: values){
            //null element check
            if(propertyGraphElement.isNull()){
                continue;
            }

            put(propertyGraphElement);

            System.out.println("bleh");
        }

        int vertexCount = 0;
        int edgeCount   = 0;

        // Output vertex records

        write();

    }

    private void put(PropertyGraphElement propertyGraphElement){
        propertyGraphElement.typeCallback(propertyGraphElementPut, vertexSet, edgeSet, edgeReducerFunction,
                vertexReducerFunction, noBiDir);
    }

    private void write(){

    }
}
