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
package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.MergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * - duplicate edges and vertices are removed
 * - each vertex is loaded into Titan and is tagged with its Titan ID and passed to the next MR job
 *   through the temp file
 * - each edge is tagged with the Titan ID of its source vertex and passed to the next MR job
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElements
 * @see com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElementPut
 */
public class TitanMergedGraphElementWrite extends MergedGraphElementWrite{
    private HashMap<Object, Long>  vertexNameToTitanID = new HashMap<>();


    /*HashMap<EdgeID, Writable> edgeSet;
    HashMap<Object, Writable> vertexSet;
    HashMap<Object, StringType> vertexLabelMap;
    Enum vertexCounter;
    Enum edgeCounter;
    Reducer.Context context;
    TitanGraph graph;
    SerializedPropertyGraphElement outValue;
    IntWritable outKey;
    KeyFunction keyFunction;

    private void initArgs(ArgumentBuilder args){
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
    }*/

    @Override
    public void write(ArgumentBuilder args)
            throws IOException, InterruptedException {
        initArgs(args);


        vertexWrite(args);

        edgeWrite(args);
    }

    public long getVertexId(com.tinkerpop.blueprints.Vertex bpVertex){
        return ((TitanElement)bpVertex).getID();
    }

    @Override
    public void vertexWrite(ArgumentBuilder args) throws IOException, InterruptedException {
        initArgs(args);

        int vertexCount = 0;

        for(Map.Entry<Object, Writable> vertex: vertexSet.entrySet()) {
            // Major operation - vertex is added to Titan and a new ID is assigned to it
            com.tinkerpop.blueprints.Vertex  bpVertex = graph.addVertex(null);

            bpVertex.setProperty("trueName", vertex.getKey().toString());

            long vertexId = getVertexId(bpVertex);//((TitanElement) bpVertex).getID();

            Vertex tempVertex = new Vertex();

            tempVertex.configure((WritableComparable) vertex.getKey(), writeVertexProperties(vertexId, vertex, bpVertex));

            outValue.init(tempVertex);
            outKey.set(keyFunction.getVertexKey(tempVertex));

            context.write(outKey, outValue);

            vertexNameToTitanID.put(vertex.getKey(), vertexId);

            vertexCount++;
        }

        context.getCounter(vertexCounter).increment(vertexCount);
    }

    @Override
    public void edgeWrite(ArgumentBuilder args)
            throws IOException, InterruptedException {
        initArgs(args);

        int edgeCount   = 0;

        for(Map.Entry<EdgeID, Writable> edge: edgeSet.entrySet()){
            Object src                  = edge.getKey().getSrc();
            Object dst                  = edge.getKey().getDst();
            String label                = edge.getKey().getLabel().toString();

            long srcTitanId = vertexNameToTitanID.get(src);

            Edge tempEdge = new Edge();

            writeEdgeProperties(srcTitanId, edge);

            tempEdge.configure((WritableComparable)  src, (WritableComparable)  dst, new StringType(label),
                    writeEdgeProperties(srcTitanId, edge));

            outValue.init(tempEdge);
            outKey.set(keyFunction.getEdgeKey(tempEdge));

            context.write(outKey, outValue);

            edgeCount++;
        }
        context.getCounter(edgeCounter).increment(edgeCount);

    }

    private PropertyMap writeVertexProperties(long vertexId, Map.Entry<Object, Writable> vertex, com.tinkerpop.blueprints.Vertex bpVertex){

        PropertyMap propertyMap = (PropertyMap) vertex.getValue();

        if(propertyMap == null){
            propertyMap = new PropertyMap();
        }

        for (Writable keyName : propertyMap.getPropertyKeys()) {
            EncapsulatedObject mapEntry = (EncapsulatedObject) propertyMap.getProperty(keyName.toString());

            bpVertex.setProperty(keyName.toString(), mapEntry.getBaseObject());
        }

        propertyMap.setProperty("TitanID", new LongType(vertexId));

        return propertyMap;
    }

    private PropertyMap writeEdgeProperties(long srcTitanId, Map.Entry<EdgeID, Writable> edge){

        PropertyMap propertyMap = (PropertyMap) edge.getValue();

        if(propertyMap == null){
            propertyMap = new PropertyMap();
        }

        propertyMap.setProperty("srcTitanID", new LongType(srcTitanId));

        return propertyMap;
    }
}
