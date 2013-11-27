package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement;

import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

    public PropertyGraphElements(){
        propertyGraphElementPut = new PropertyGraphElementPut();
    }

    public PropertyGraphElements(Functional vertexReducerFunction, Functional edgeReducerFunction,
                                 Reducer.Context context,TitanGraph graph,
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
    }


    public void mergeDuplicates(Iterable<SerializedPropertyGraphElement> values)
            throws IOException, InterruptedException {
        Iterator<SerializedPropertyGraphElement> valueIterator = values.iterator();

        for(SerializedPropertyGraphElement serializedPropertyGraphElement: values){
            PropertyGraphElement propertyGraphElement = serializedPropertyGraphElement.graphElement();

            //null element check
            if(propertyGraphElement.isNull()){
                continue;
            }

            LOG.info("put");
            put(propertyGraphElement);
        }

        LOG.info("write");
        write();
    }

    private void put(PropertyGraphElement propertyGraphElement){
        propertyGraphElement.typeCallback(propertyGraphElementPut, edgeSet, vertexSet, edgeReducerFunction,
                vertexReducerFunction, noBiDir);
    }

    protected long getVertexId(com.tinkerpop.blueprints.Vertex  bpVertex){
        if(bpVertex != null){
            return ((TitanElement) bpVertex).getID();
        }
        return 0L;
    }

    private void write() throws IOException, InterruptedException {
        int vertexCount = 0;
        int edgeCount   = 0;


        LOG.info("top vertex set");

        Iterator<Map.Entry<Object, Writable>> vertexIterator = vertexSet.entrySet().iterator();


        while (vertexIterator.hasNext()) {

            Map.Entry v     = vertexIterator.next();
            LOG.info("key: " + v.getKey().toString());
            // Major operation - vertex is added to Titan and a new ID is assigned to it

            com.tinkerpop.blueprints.Vertex  bpVertex = graph.addVertex(null);

            bpVertex.setProperty("trueName", v.getKey().toString());

            PropertyMap propertyMap = (PropertyMap) v.getValue();

            for (Writable keyName : propertyMap.getPropertyKeys()) {
                EncapsulatedObject mapEntry = (EncapsulatedObject) propertyMap.getProperty(keyName.toString());

                bpVertex.setProperty(keyName.toString(), mapEntry.getBaseObject());
            }

            long vertexId = ((TitanElement) bpVertex).getID();

            Vertex vertex = new Vertex();
            propertyMap.setProperty("TitanID", new LongType(vertexId));
            vertex.configure((WritableComparable) v.getKey(), propertyMap);

            outValue.init(vertex);
            outKey.set(keyFunction.getVertexKey(vertex));

            context.write(outKey, outValue);

            vertexNameToTitanID.put(v.getKey(), vertexId);

            vertexCount++;
        }


        /*Iterator<Map.Entry<Object, Writable>> vertexIterator = vertexSet.entrySet().iterator();


        while (vertexIterator.hasNext()) {

            Map.Entry vertex    = vertexIterator.next();
            LOG.info("ITER ");
            // Major operation - vertex is added to Titan and a new ID is assigned to it
            com.tinkerpop.blueprints.Vertex  bpVertex = graph.addVertex(null);

            bpVertex.setProperty("trueName", vertex.getKey().toString());

            PropertyMap propertyMap = (PropertyMap) vertex.getValue();





            for (Writable keyName : propertyMap.getPropertyKeys()) {
                EncapsulatedObject mapEntry = (EncapsulatedObject) propertyMap.getProperty(keyName.toString());

                bpVertex.setProperty(keyName.toString(), mapEntry.getBaseObject());
            }


            long vertexId = getVertexId(bpVertex);




            //bpVertex.setProperty("trueName", vertex.getKey().toString());



            //tempVertex.configure((WritableComparable) vertex.getKey(), writeVertexProperties(vertexId, vertex, bpVertex));
            Vertex tempVertex = new Vertex();
            propertyMap.setProperty("TitanID", new LongType(vertexId));
            tempVertex.configure((WritableComparable) vertex.getKey(), propertyMap);

            outValue.init(tempVertex);
            outKey.set(keyFunction.getVertexKey(tempVertex));

            context.write(outKey, outValue);

            vertexNameToTitanID.put(vertex.getKey(), vertexId);

            vertexCount++;
        }*/
        LOG.info("bottom vertex set");
        this.context.getCounter(vertexCounter).increment(vertexCount);

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
        this.context.getCounter(edgeCounter).increment(edgeCount);

    }

    private PropertyMap writeVertexProperties(long vertexId, Map.Entry<Object, Writable> vertex, com.tinkerpop.blueprints.Vertex bpVertex){
        PropertyMap propertyMap = (PropertyMap) vertex.getValue();
        if(propertyMap == null){
            propertyMap = new PropertyMap();
        }

        for (Writable keyName : propertyMap.getPropertyKeys()) {
            EncapsulatedObject mapEntry = (EncapsulatedObject) propertyMap.getProperty(keyName.toString());

            LOG.info("keyname: " + keyName.toString());
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
