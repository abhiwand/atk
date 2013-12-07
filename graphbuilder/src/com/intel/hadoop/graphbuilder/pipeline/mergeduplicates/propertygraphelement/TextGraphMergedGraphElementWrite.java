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

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.MergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * - each vertex is loaded into Titan and is tagged with its Titan ID and passed to the next MR job
 *   through the temp file
 * - each edge is tagged with the Titan ID of its source vertex and passed to the next MR job
 *
 * @see PropertyGraphElements
 * @see PropertyGraphElementPut
 */
public class TextGraphMergedGraphElementWrite implements MergedGraphElementWrite {
    MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    public void write(HashMap<EdgeID, Writable> edgeSet, HashMap<Object, Writable> vertexSet,
                      HashMap<Object, StringType> vertexLabelMap, Enum vertexCounter,
                      Enum edgeCounter, Reducer.Context context, TitanGraph graph,
                      SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction)
            throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);

        vertexWrite(vertexSet, vertexCounter, context, graph, outValue, outKey, keyFunction);

        edgeWrite(edgeSet, edgeCounter, context, graph, outValue, outKey, keyFunction);
    }

    @Override
    public void vertexWrite(HashMap<Object, Writable> vertexSet, Enum counter, Reducer.Context context, TitanGraph graph, SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction) throws IOException, InterruptedException {

        int vertexCount = 0;
        String outPath = new String("edata/part");

        // Output vertex records

        Iterator<Map.Entry<Object, Writable>> vertexIterator = vertexSet.entrySet().iterator();

        for (Map.Entry<Object, Writable> vertex: vertexSet.entrySet()) {

            Text value = new Text(vertex.getKey().toString() + "\t" + vertex.getValue().toString());

            multipleOutputs.write(NullWritable.get(), value, outPath);
            vertexCount++;
        }

        context.getCounter(counter).increment(vertexCount);
    }

    @Override
    public void edgeWrite(HashMap<EdgeID, Writable> edgeSet, Enum counter, Reducer.Context context, TitanGraph graph, SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction) throws IOException, InterruptedException {

        Iterator<Map.Entry<EdgeID, Writable>> edgeIterator = edgeSet.entrySet().iterator();

        int edgeCount   = 0;
        String outPath = new String("edata/part");

        for(Map.Entry<EdgeID, Writable> edge: edgeSet.entrySet()) {

            Text value = new Text(edge.getKey().getSrc() + "\t" + edge.getKey().getDst() + "\t" + edge.getKey().getLabel()
                    + "\t" + edge.getValue().toString());

            multipleOutputs.write(NullWritable.get(), value, outPath);
            edgeCount++;
        }

        context.getCounter(counter).increment(edgeCount);
    }



}
