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

/**
 # - duplicate edges and vertices are removed
 * - each vertex is loaded into Titan and is tagged with its Titan ID and passed to the next MR job
 *   through the temp file
 * - each edge is tagged with the Titan ID of its source vertex and passed to the next MR job
 *
 * @see PropertyGraphElementPut
 */
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

    /**
     * merges duplicate graph elements
     * @param propertyGraphElement
     */
    private void put(PropertyGraphElement propertyGraphElement){
        propertyGraphElement.typeCallback(propertyGraphElementPut, edgeSet, vertexSet, edgeReducerFunction,
                vertexReducerFunction, noBiDir);
    }
}
