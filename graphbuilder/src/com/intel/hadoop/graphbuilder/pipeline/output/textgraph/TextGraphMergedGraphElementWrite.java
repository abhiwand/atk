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
package com.intel.hadoop.graphbuilder.pipeline.output.textgraph;

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.pipeline.output.MergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * - each vertex is loaded into Titan and is tagged with its Titan ID and passed to the next MR job
 *   through the temp file
 * - each edge is tagged with the Titan ID of its source vertex and passed to the next MR job
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElements
 * @see com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElementMerge
 */
public class TextGraphMergedGraphElementWrite extends MergedGraphElementWrite {
    MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    public void write(ArgumentBuilder args)
            throws IOException, InterruptedException {
        initArgs(args);

        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);

        vertexWrite(args);

        edgeWrite(args);
    }

    @Override
    public void vertexWrite(ArgumentBuilder args) throws IOException, InterruptedException {
        initArgs(args);

        int vertexCount = 0;
        String outPath = new String("edata/part");

        // Output vertex records

        Iterator<Map.Entry<Object, Writable>> vertexIterator = vertexSet.entrySet().iterator();

        for (Map.Entry<Object, Writable> vertex: vertexSet.entrySet()) {

            Text value = new Text(vertex.getKey().toString() + "\t" + vertex.getValue().toString());

            multipleOutputs.write(NullWritable.get(), value, outPath);
            vertexCount++;
        }

        context.getCounter(edgeCounter).increment(vertexCount);
    }

    @Override
    public void edgeWrite(ArgumentBuilder args) throws IOException, InterruptedException {
        initArgs(args);

        Iterator<Map.Entry<EdgeID, Writable>> edgeIterator = edgeSet.entrySet().iterator();

        int edgeCount   = 0;
        String outPath = new String("edata/part");

        for(Map.Entry<EdgeID, Writable> edge: edgeSet.entrySet()) {

            Text value = new Text(edge.getKey().getSrc() + "\t" + edge.getKey().getDst() + "\t" + edge.getKey().getLabel()
                    + "\t" + edge.getValue().toString());

            multipleOutputs.write(NullWritable.get(), value, outPath);
            edgeCount++;
        }

        context.getCounter(edgeCounter).increment(edgeCount);
    }
}
