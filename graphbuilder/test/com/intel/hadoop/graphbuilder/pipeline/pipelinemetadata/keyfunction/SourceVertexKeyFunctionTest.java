package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;

public class SourceVertexKeyFunctionTest {

    @Test
    public void testGetEdgeKey() throws Exception {

        SourceVertexKeyFunction keyFunction = new SourceVertexKeyFunction();

        StringType sourceId = new StringType("The Sound");
        StringType destId1  = new StringType(" and the Fury");
        StringType destId2  = new StringType(" of Silence");

        StringType faulkner = new StringType("label");

        Edge<StringType> edge1 = new Edge<StringType>(sourceId, destId1, faulkner);
        Edge<StringType> edge2 = new Edge<StringType>(sourceId, destId2, new StringType("Simon"));

        assertEquals(keyFunction.getEdgeKey(edge1), keyFunction.getEdgeKey(edge2));

        StringType altSourceId = new StringType("The Curry");
        Edge<StringType> oddEdgeOut = new Edge<StringType>(altSourceId, destId1, faulkner);

        // technically, this could legally happen, if the underlying Java hash function sent the edge IDs to the same
        // integer... but if that's what happens, we'd like to know about it
        assert(keyFunction.getEdgeKey(edge1) != keyFunction.getEdgeKey(oddEdgeOut));
    }

    @Test
    public void testGetVertexKey() throws Exception {

        SourceVertexKeyFunction keyFunction = new SourceVertexKeyFunction();

        StringType name      = new StringType("Tex, Ver Tex.");
        StringType otherName = new StringType("huh?");


        Vertex<StringType> vertex       = new Vertex<StringType>(name);
        Vertex<StringType> vertexClone  = new Vertex<StringType>(name);
        Vertex<StringType> oddVertexOut = new Vertex<StringType>(otherName);

        assertEquals(keyFunction.getVertexKey(vertex), keyFunction.getVertexKey(vertexClone));

        // technically, the two vertices could have the same key value, if the underlying Java hash function sent their
        // IDs to the same integer... but if that's the case we'd like to know about it

        assert(keyFunction.getVertexKey(vertex) != keyFunction.getVertexKey(oddVertexOut));
    }

}
