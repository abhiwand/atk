package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.PropertyGraphElement;

import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.DestinationVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.Functional;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PropertyGraphElements {
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
    private Class                  outClass;

    private final KeyFunction keyFunction = new DestinationVertexKeyFunction();

    private Enum vertexCounter;
    private Enum edgeCounter;

    public PropertyGraphElements(){
        propertyGraphElementPut = new PropertyGraphElementPut();
    }

    public PropertyGraphElements(Functional vertexReducerFunction, Functional edgeReducerFunction,
                                 Reducer.Context context,TitanGraph graph, Class outClass,
                                 SerializedPropertyGraphElement outValue, Enum edgeCounter,  Enum vertexCounter)
            throws IllegalAccessException, InstantiationException {

        this();

        this.vertexReducerFunction = vertexReducerFunction;
        this.edgeReducerFunction = edgeReducerFunction;

        this.context = context;
        this.noBiDir = context.getConfiguration().getBoolean("noBiDir", false);

        this.graph = graph;
        this.outClass = outClass;
        this.outKey   = new IntWritable();
        this.outValue   = outValue;
        this.vertexNameToTitanID = new HashMap<Object, Long>();
        this.vertexCounter = vertexCounter;
        this.edgeCounter = edgeCounter;
    }



    public void mergeDuplicates(Iterable<com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement> values)
            throws IOException, InterruptedException {
        Iterator<com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement> valueIterator = values.iterator();

        for(PropertyGraphElement propertyGraphElement: values){
            //null element check
            if(propertyGraphElement.isNull()){
                continue;
            }

            put(propertyGraphElement);

            System.out.println("bleh");
        }

        write();
    }

    private void put(PropertyGraphElement propertyGraphElement){
        propertyGraphElement.typeCallback(propertyGraphElementPut, vertexSet, edgeSet, edgeReducerFunction,
                vertexReducerFunction, noBiDir);
    }

    private void write() throws IOException, InterruptedException {
        int vertexCount = 0;
        int edgeCount   = 0;

        for(Map.Entry<Object, Writable> vertex: vertexSet.entrySet()){

            // Major operation - vertex is added to Titan and a new ID is assigned to it

            com.tinkerpop.blueprints.Vertex  bpVertex = graph.addVertex(null);

            bpVertex.setProperty("trueName", vertex.getKey().toString());

            PropertyMap propertyMap = (PropertyMap) vertex.getValue();

            for (Writable keyName : propertyMap.getPropertyKeys()) {
                EncapsulatedObject mapEntry = (EncapsulatedObject) propertyMap.getProperty(keyName.toString());

                bpVertex.setProperty(keyName.toString(), mapEntry.getBaseObject());
            }

            long vertexId = ((TitanElement) bpVertex).getID();

            Vertex tempVertex = new Vertex();
            propertyMap.setProperty("TitanID", new LongType(vertexId));
            tempVertex.configure((WritableComparable) vertex.getKey(), propertyMap);

            outValue.init(tempVertex);
            outKey.set(keyFunction.getVertexKey(tempVertex));

            context.write(outKey, outValue);

            vertexNameToTitanID.put(vertex.getKey(), vertexId);

            vertexCount++;
        }
        this.context.getCounter(vertexCounter).increment(vertexCount);

        for(Map.Entry<EdgeID, Writable> edge: edgeSet.entrySet()){
            Object src                  = edge.getKey().getSrc();
            Object dst                  = edge.getKey().getDst();
            String label                = edge.getKey().getLabel().toString();

            PropertyMap propertyMap = (PropertyMap) edge.getValue();

            long srcTitanId = vertexNameToTitanID.get(src);

            Edge tempEdge = new Edge();

            propertyMap.setProperty("srcTitanID", new LongType(srcTitanId));

            tempEdge.configure((WritableComparable)  src, (WritableComparable)  dst, new StringType(label), propertyMap);

            outValue.init(tempEdge);
            outKey.set(keyFunction.getEdgeKey(tempEdge));

            context.write(outKey, outValue);

            edgeCount++;
        }
        this.context.getCounter(edgeCounter).increment(edgeCount);

    }
}
