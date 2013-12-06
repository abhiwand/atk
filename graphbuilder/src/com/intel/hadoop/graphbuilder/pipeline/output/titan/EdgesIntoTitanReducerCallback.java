package com.intel.hadoop.graphbuilder.pipeline.output.titan;


import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.graphelements.callbacks.PropertyGraphElementTypeCallback;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;

/**
 * Apply reduce on edges and vertices, remove self and (or merge) duplicate edges.
 *
 * @see EdgesIntoTitanReducer
 */
public class EdgesIntoTitanReducerCallback implements PropertyGraphElementTypeCallback{
    private HashMap<EdgeID, Writable> edgePropertyTable;
    private HashMap<Object, Long> vertexNameToTitanID;
    @Override
    public HashMap<EdgeID, Writable> edge(PropertyGraphElement propertyGraphElement, Object... args) {
        initArguments(args);

        // Apply reduce on edges, remove self and (or merge) duplicate edges.
        // Optionally remove bidirectional edge.

        Edge edge   = (Edge) propertyGraphElement;
        EdgeID edgeID = new EdgeID(edge.getSrc(), edge.getDst(), edge.getEdgeLabel());

        edgePropertyTable.put(edgeID, edge.getProperties());

        return edgePropertyTable;
    }

    @Override
    public HashMap<Object, Long> vertex(PropertyGraphElement propertyGraphElement, Object... args) {
        initArguments(args);

        // Apply reduce on vertex

        Vertex vertex = (Vertex) propertyGraphElement;

        Object      vertexId      = vertex.getVertexId();
        PropertyMap propertyMap   = vertex.getProperties();
        long        vertexTitanId = ((LongType) propertyMap.getProperty("TitanID")).get();
        vertexNameToTitanID.put(vertexId, vertexTitanId);
        return vertexNameToTitanID;
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, Object... args) {
        return null;
    }

    private void initArguments(Object ... args){
        if(args.length == 2){
            edgePropertyTable = (HashMap<EdgeID, Writable>)args[0];
            vertexNameToTitanID = (HashMap<Object, Long>)args[1];
        }else{
            throw new IllegalArgumentException("Incorrect number of arguments expect exactly 2 given: " + args.length );
        }
    }
}
