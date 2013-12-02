package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement;

import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.MergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.DestinationVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.Functional;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PropertyGraphElements {
    private static final Logger LOG = Logger.getLogger(PropertyGraphElements.class);
    private PropertyGraphElementPut propertyGraphElementPut;

    HashMap<EdgeID, Writable>   edgeSet       = new HashMap<>();
    HashMap<Object, Writable>   vertexSet     = new HashMap<>();

    private Reducer.Context context;

    private boolean noBiDir;

    private Functional edgeReducerFunction;
    private Functional vertexReducerFunction;
    private TitanGraph graph;
    private HashMap<Object, Long>  vertexNameToTitanID;
    private IntWritable outKey;
    private SerializedPropertyGraphElement outValue;
    //private Class                  outClass;

    private final KeyFunction keyFunction = new DestinationVertexKeyFunction();

    private Enum vertexCounter;
    private Enum edgeCounter;

    private MergedGraphElementWrite mergedGraphElementWrite;

    public PropertyGraphElements(){
        propertyGraphElementPut = new PropertyGraphElementPut();

    }

    public PropertyGraphElements(MergedGraphElementWrite mergedGraphElementWrite, Functional vertexReducerFunction,
                                 Functional edgeReducerFunction, Reducer.Context context,TitanGraph graph,
                                 SerializedPropertyGraphElement outValue, Enum edgeCounter,  Enum vertexCounter){

        this();
        this.vertexReducerFunction = vertexReducerFunction;
        this.edgeReducerFunction = edgeReducerFunction;
        this.context = context;
        this.noBiDir = context.getConfiguration().getBoolean("noBiDir", false);
        this.graph = graph;
        this.outKey   = new IntWritable();
        this.outValue   = outValue;
        this.vertexNameToTitanID = new HashMap<Object, Long>();
        this.vertexCounter = vertexCounter;
        this.edgeCounter = edgeCounter;
        this.mergedGraphElementWrite = mergedGraphElementWrite;
    }


    public void mergeDuplicates(Iterable<SerializedPropertyGraphElement> values)
            throws IOException, InterruptedException {

        edgeSet       = new HashMap<>();
        vertexSet     = new HashMap<>();

        for(SerializedPropertyGraphElement serializedPropertyGraphElement: values){
            PropertyGraphElement propertyGraphElement = serializedPropertyGraphElement.graphElement();

            //null element check
            if(propertyGraphElement.isNull()){
                continue;
            }

            put(propertyGraphElement);
        }
    }

    public void write() throws IOException, InterruptedException {
        mergedGraphElementWrite.write(edgeSet, vertexSet, vertexCounter, edgeCounter, context, graph, outValue, outKey,
                keyFunction);
    }

    private void put(PropertyGraphElement propertyGraphElement){
        propertyGraphElement.typeCallback(propertyGraphElementPut, edgeSet, vertexSet, edgeReducerFunction,
                vertexReducerFunction, noBiDir);
    }
}
