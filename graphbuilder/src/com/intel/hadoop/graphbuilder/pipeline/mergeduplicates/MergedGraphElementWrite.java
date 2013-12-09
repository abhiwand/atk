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
package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

/**
 * simple interface for writing the merged edges and vertices
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanMergedGraphElementWrite
 * @see com.intel.hadoop.graphbuilder.pipeline.output.textgraph.TextGraphMergedGraphElementWrite
 */
public abstract class MergedGraphElementWrite {
    protected HashMap<EdgeID, Writable> edgeSet;
    protected HashMap<Object, Writable> vertexSet;
    protected HashMap<Object, StringType> vertexLabelMap;
    protected Enum vertexCounter;
    protected Enum edgeCounter;
    protected Reducer.Context context;
    protected TitanGraph graph;
    protected SerializedPropertyGraphElement outValue;
    protected IntWritable outKey;
    protected KeyFunction keyFunction;

    protected  void initArgs(ArgumentBuilder args){
        edgeSet = (HashMap<EdgeID, Writable>)args.get("edgeSet");
        vertexSet = (HashMap<Object, Writable>)args.get("vertexSet");
        vertexLabelMap = (HashMap<Object, StringType>)args.get("vertexLabelMap");

        vertexCounter = (Enum)args.get("vertexCounter");
        edgeCounter = (Enum)args.get("edgeCounter");

        context = (Reducer.Context)args.get("context");

        graph = (TitanGraph)args.get("graph");

        outValue = (SerializedPropertyGraphElement)args.get("outValue");
        outKey = (IntWritable)args.get("outKey");
        keyFunction = (KeyFunction)args.get("keyFunction");
    }

    /**
     *
     * @param edgeSet merged edge hashmap
     * @param vertexSet merged vetices hashmap
     * @param vertexCounter counter to be increment when after succesfull vertex write
     * @param edgeCounter counter to be increment when after succesfull edge write
     * @param context the reducer context
     * @param graph titan graph
     * @param outValue instance of context.getMapOutputValueClass
     * @param outKey instance of teh out key usually IntWritable
     * @param keyFunction key function for creating the context.write key
     * @throws IOException
     * @throws InterruptedException
     */
    /*HashMap<EdgeID, Writable> edgeSet, HashMap<Object, Writable> vertexSet,
                      HashMap<Object, StringType> vertexLabelMap,
                      Enum vertexCounter, Enum edgeCounter, Reducer.Context context, TitanGraph graph,
                      SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction
                      */
    public abstract void write(ArgumentBuilder args)
            throws IOException, InterruptedException;


    /*
    *
    * HashMap<Object, Writable> vertexSet,
                            Enum counter, Reducer.Context context,
                            TitanGraph graph, SerializedPropertyGraphElement outValue,
                            IntWritable outKey, KeyFunction keyFunction
    * */
    public abstract void vertexWrite(ArgumentBuilder args) throws IOException, InterruptedException;

    /*
    HashMap<EdgeID, Writable> edgeSet, Enum counter,  Reducer.Context context,
                          TitanGraph graph, SerializedPropertyGraphElement outValue,
                          IntWritable outKey, KeyFunction keyFunction
     */
    public abstract void edgeWrite(ArgumentBuilder args) throws IOException, InterruptedException;

}
