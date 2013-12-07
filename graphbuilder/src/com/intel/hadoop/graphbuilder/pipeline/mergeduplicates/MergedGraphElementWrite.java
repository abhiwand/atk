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
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

/**
 * simple interface for writing the merged edges and vertices
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.TitanMergedGraphElementWrite
 * @see com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.TextGraphMergedGraphElementWrite
 */
public interface MergedGraphElementWrite {

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
    public void write(HashMap<EdgeID, Writable> edgeSet, HashMap<Object, Writable> vertexSet,
                      HashMap<Object, StringType> vertexLabelMap,
                      Enum vertexCounter, Enum edgeCounter, Reducer.Context context, TitanGraph graph,
                      SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction)
            throws IOException, InterruptedException;

    public void vertexWrite(HashMap<Object, Writable> vertexSet,
                            Enum counter, Reducer.Context context,
                            TitanGraph graph, SerializedPropertyGraphElement outValue,
                            IntWritable outKey, KeyFunction keyFunction) throws IOException, InterruptedException;

    public void edgeWrite(HashMap<EdgeID, Writable> edgeSet, Enum counter,  Reducer.Context context,
                          TitanGraph graph, SerializedPropertyGraphElement outValue,
                          IntWritable outKey, KeyFunction keyFunction) throws IOException, InterruptedException;

}
