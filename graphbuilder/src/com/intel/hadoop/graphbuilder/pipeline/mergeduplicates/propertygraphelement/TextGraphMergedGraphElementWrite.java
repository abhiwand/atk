package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement;

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.MergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
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


    @Override
    public void write(HashMap<EdgeID, Writable> edgeSet, HashMap<Object, Writable> vertexSet, Enum vertexCounter,
                      Enum edgeCounter, Reducer.Context context, TitanGraph graph,
                      SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction)
            throws IOException, InterruptedException {

        vertexWrite(vertexSet, vertexCounter, context, graph, outValue, outKey, keyFunction);

        edgeWrite(edgeSet, edgeCounter, context, graph, outValue, outKey, keyFunction);
    }

    @Override
    public void vertexWrite(HashMap<Object, Writable> vertexSet, Enum counter, Reducer.Context context, TitanGraph graph, SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction) throws IOException, InterruptedException {
        MultipleOutputs<NullWritable, Text> multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);

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
        MultipleOutputs<NullWritable, Text> multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);

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
