package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public interface MergedGraphElementWrite {

    public void write(HashMap<EdgeID, Writable> edgeSet, HashMap<Object, Writable> vertexSet, Enum vertexCounter,
                      Enum edgeCounter, Reducer.Context context, TitanGraph graph,
                      SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction)
            throws IOException, InterruptedException;

    public void vertexWrite(HashMap<Object, Writable> vertexSet, Enum counter, Reducer.Context context,
                            TitanGraph graph, SerializedPropertyGraphElement outValue,
                            IntWritable outKey, KeyFunction keyFunction) throws IOException, InterruptedException;

    public void edgeWrite(HashMap<EdgeID, Writable> edgeSet,Enum counter,  Reducer.Context context,
                          TitanGraph graph, SerializedPropertyGraphElement outValue,
                          IntWritable outKey, KeyFunction keyFunction) throws IOException, InterruptedException;

}
