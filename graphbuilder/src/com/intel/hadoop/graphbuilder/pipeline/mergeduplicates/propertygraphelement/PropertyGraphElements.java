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
    private IntWritable outKey;
    private SerializedPropertyGraphElement outValue;

    private final KeyFunction keyFunction = new DestinationVertexKeyFunction();

    private Enum vertexCounter;
    private Enum edgeCounter;

    private MergedGraphElementWrite mergedGraphElementWrite;

    public PropertyGraphElements(){
        propertyGraphElementPut = new PropertyGraphElementPut();

    }

    /**
     *
     * <p>
     *     expected arguments in the Argument builder
     * <ul>
     *      <li>edgeSet - hashmap with the current list of merged edges</li>
     *      <li>vertexSet - hashmap with the current list of merged vertices</li>
     *      <li>edgeReducerFunction - optional edge reducer function</li>
     *      <li>vertexReducerFunction - optional vertex reducer function</li>
     *      <li>context - Reducer context</li>
     *      <li>noBidir - are we cleaning bidirectional edges. if true then remove bidirectional edge</li>
     *      <li>graph - Titan graph instance</li>
     *      <li>outValue - instance of </li>
     *      <li>vertexCounter - to increment when we write a vertex</li>
     *      <li>edgeCounter - to increment when we write an edge</li>
     *      <li>vertexLabelMap - list of vertex labels to be used for writing rdf output</li>
     *      <li>noBiDir - are we cleaning bidirectional edges. if true then remove bidirectional edge</li>
     *      <li>mergedGraphElementWrite - the write class to call when we need to output our edges and vertices</li>
     * </ul>
     * </p>
     * @param args an ArgumentBuilder with all the necessary arguments
     */
    public PropertyGraphElements(ArgumentBuilder args){
        this();
        this.vertexReducerFunction = (Functional)args.get("vertexReducerFunction");;
        this.edgeReducerFunction = (Functional)args.get("edgeReducerFunction");
        this.context = (Reducer.Context)args.get("context");
        this.noBiDir = context.getConfiguration().getBoolean("noBiDir", false);
        this.graph = (TitanGraph)args.get("graph");
        this.outKey   = new IntWritable();
        this.outValue   = (SerializedPropertyGraphElement)args.get("outValue");
        this.vertexCounter = (Enum)args.get("vertexCounter");
        this.edgeCounter = (Enum)args.get("edgeCounter");
        this.mergedGraphElementWrite = (MergedGraphElementWrite)args.get("mergedGraphElementWrite");
    }

    /**
     *
     * @param mergedGraphElementWrite the write class to call when we need to output our edges and vertices
     * @param vertexReducerFunction optional vertex reducer function
     * @param edgeReducerFunction optional edge reducer function
     * @param context Reducer.Context for writing
     * @param graph Titan graph element
     * @param outValue outValue type usually (SerializedPropertyGraphElement)
     * @param edgeCounter the edge counter to increment when writing
     * @param vertexCounter the vertex counter to increment when writing
     */
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

            //if we have a null element skip it and go to the next one
            if(propertyGraphElement.isNull()){
                continue;
            }

            //try to add the graph element to the existing set of vertices or edges
            //PropertyGraphElementPut will take care of switching between edge and vertex
            put(propertyGraphElement);
        }
    }

    /**
     * Call MergedGraphElementWrite function the class  was initiated with to write the edges and vertices.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void write() throws IOException, InterruptedException {

        mergedGraphElementWrite.write(ArgumentBuilder.newArguments().with("edgeSet", edgeSet)
                .with("vertexSet", vertexSet).with("vertexLabelMap", vertexLabelMap)
                .with("vertexCounter", vertexCounter).with("edgeCounter", edgeCounter).with("context", context)
                .with("graph", graph).with("outValue", outValue).with("outKey", outKey).with("keyFunction", keyFunction));
    }

    /**
     * remove duplicate edges/vertices and merge their property maps
     *
     * @param propertyGraphElement the graph element to add to our existing vertexSet or edgeSet
     */
    private void put(PropertyGraphElement propertyGraphElement){
        propertyGraphElement.typeCallback(propertyGraphElementPut,
                ArgumentBuilder.newArguments().with("edgeSet", edgeSet).with("vertexSet", vertexSet)
                    .with("edgeReducerFunction", edgeReducerFunction)
                    .with("vertexReducerFunction", vertexReducerFunction)
                    .with("noBiDir", noBiDir));
    }


}
