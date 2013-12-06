package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement;

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
 * @see PropertyGraphElements
 * @see PropertyGraphElementPut
 */
public class TitanMergedGraphElementWrite implements MergedGraphElementWrite{
    private HashMap<Object, Long>  vertexNameToTitanID = new HashMap<>();

    @Override
    public void write(HashMap<EdgeID, Writable> edgeSet, HashMap<Object, Writable> vertexSet, Enum vertexCounter,
                      Enum edgeCounter, Reducer.Context context, TitanGraph graph,
                      SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction)
            throws IOException, InterruptedException {

        vertexWrite(vertexSet, vertexCounter, context, graph, outValue, outKey, keyFunction);

        edgeWrite(edgeSet, edgeCounter, context, graph, outValue, outKey, keyFunction);
    }

    public long getVertexId(com.tinkerpop.blueprints.Vertex bpVertex){
        return ((TitanElement)bpVertex).getID();
    }

    @Override
    public void vertexWrite(HashMap<Object, Writable> vertexSet,Enum counter, Reducer.Context context, TitanGraph graph,
                            SerializedPropertyGraphElement outValue,
                            IntWritable outKey, KeyFunction keyFunction) throws IOException, InterruptedException {
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

        context.getCounter(counter).increment(vertexCount);
    }

    @Override
    public void edgeWrite(HashMap<EdgeID, Writable> edgeSet,Enum counter, Reducer.Context context, TitanGraph graph,
                          SerializedPropertyGraphElement outValue, IntWritable outKey, KeyFunction keyFunction)
            throws IOException, InterruptedException {
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
        context.getCounter(counter).increment(edgeCount);

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
