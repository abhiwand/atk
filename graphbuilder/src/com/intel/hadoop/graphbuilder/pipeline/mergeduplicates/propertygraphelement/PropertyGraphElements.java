/**
 * Copyright (C) 2012 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement;

import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.MergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.DestinationVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.intel.hadoop.graphbuilder.util.Functional;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

/**
 * Coordinates the tasks of merging edges and vertices and writing them to their desired output.
 *
 * <p>
 * The merge phase is common across all three graph outputs and doesn't need to be defined but the writing class needs
 * to be declared during instantiation.
 *
 * <b>implemented write classes</b>
 * <ul>
 *     <li>RDFGraphMergedGraphElementWrite - write edges/vertices to rdf format</li>
 *     <li>TitanMergedGraphElementWrite - each vertex is loaded into Titan,
 *      each edge is tagged with the Titan ID of its source vertex and passed to the next MR job</li>
 *     <li>TextGraphMergedGraphElementWrite - write edges/vertices to tab delimited format</li>
 * </ul>
 *
 * </p>
 *
 * @see PropertyGraphElementPut
 * @see MergedGraphElementWrite
 */
public class PropertyGraphElements {
    private static final Logger LOG = Logger.getLogger(PropertyGraphElements.class);
    private PropertyGraphElementPut propertyGraphElementPut;

    HashMap<EdgeID, Writable>   edgeSet       = new HashMap<>();
    HashMap<Object, Writable>   vertexSet     = new HashMap<>();
    HashMap<Object, StringType> vertexLabelMap = new HashMap();

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

    public PropertyGraphElements(ArgumentBuilder argumentBuilder){
        this();
        this.vertexReducerFunction = (Functional)argumentBuilder.get("vertexReducerFunction");;
        this.edgeReducerFunction = (Functional)argumentBuilder.get("edgeReducerFunction");
        this.context = (Reducer.Context)argumentBuilder.get("context");
        this.noBiDir = context.getConfiguration().getBoolean("noBiDir", false);
        this.graph = (TitanGraph)argumentBuilder.get("graph");
        this.outKey   = new IntWritable();
        this.outValue   = (SerializedPropertyGraphElement)argumentBuilder.get("outValue");
        this.vertexNameToTitanID = new HashMap<Object, Long>();
        this.vertexCounter = (Enum)argumentBuilder.get("vertexCounter");
        this.edgeCounter = (Enum)argumentBuilder.get("edgeCounter");
        this.mergedGraphElementWrite = (MergedGraphElementWrite)argumentBuilder.get("mergedGraphElementWrite");
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

    /**
     * called from Reducer to merge edges and vertices
     *
     * @see com.intel.hadoop.graphbuilder.pipeline.output.titan.VerticesIntoTitanReducer
     *
     * @param values
     * @throws IOException
     * @throws InterruptedException
     */
    public void mergeDuplicates(Iterable<SerializedPropertyGraphElement> values)
            throws IOException, InterruptedException {

        edgeSet       = new HashMap<>();
        vertexSet     = new HashMap<>();
        vertexLabelMap = new HashMap<>();

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

        mergedGraphElementWrite.write(ArgumentBuilder.newArguments().with("edgeSet", edgeSet)
                .with("vertexSet", vertexSet).with("vertexLabelMap", vertexLabelMap)
                .with("vertexCounter", vertexCounter).with("edgeCounter", edgeCounter).with("context", context)
                .with("graph", graph).with("outValue", outValue).with("outKey", outKey).with("keyFunction", keyFunction));
    }

    /**
     * merges duplicate graph elements
     * @param propertyGraphElement
     */
    private void put(PropertyGraphElement propertyGraphElement){
        propertyGraphElement.typeCallback(propertyGraphElementPut,
                ArgumentBuilder.newArguments().with("edgeSet", edgeSet).with("vertexSet", vertexSet)
                    .with("edgeReducerFunction", edgeReducerFunction)
                    .with("vertexReducerFunction", vertexReducerFunction)
                    .with("noBiDir", noBiDir));
    }


}
