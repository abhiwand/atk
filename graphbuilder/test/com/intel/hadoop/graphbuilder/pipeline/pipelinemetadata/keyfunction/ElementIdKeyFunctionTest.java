package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;
import static junit.framework.Assert.assertSame;

public class ElementIdKeyFunctionTest {

    @Test
    public void testGetEdgeKey() throws Exception {

        ElementIdKeyFunction keyFunction = new ElementIdKeyFunction();

        StringType sourceId1  = new StringType("Scooby Doo Meets ");
        StringType sourceId2  = new StringType("The Further Adventures of ");
        StringType destId1    = new StringType(" Batman and Robin");
        StringType destId2    = new StringType(" Don Amece");

        StringType label1  = new StringType("awesome");
        StringType label2  = new StringType("most awesome");


        Edge<StringType> edge      = new Edge<StringType>(sourceId1, destId1, label1);
        Edge<StringType> edgeClone = new Edge<StringType>(sourceId1, destId1, label1);

        assertEquals(keyFunction.getEdgeKey(edge), keyFunction.getEdgeKey(edgeClone));


        // technically, one of these could legally fail,
        // if the underlying Java hash function sent the edge IDs to the same
        // integer... but if that's what happens, we'd like to know about it

        Edge<StringType> edge211      = new Edge<StringType>(sourceId2, destId1, label1);
        Edge<StringType> edge121      = new Edge<StringType>(sourceId1, destId2, label1);
        Edge<StringType> edge112      = new Edge<StringType>(sourceId1, destId1, label2);

        assert(keyFunction.getEdgeKey(edge) != keyFunction.getEdgeKey(edge211));
        assert(keyFunction.getEdgeKey(edge) != keyFunction.getEdgeKey(edge121));
        assert(keyFunction.getEdgeKey(edge) != keyFunction.getEdgeKey(edge112));
    }

    @Test
    public void testGetVertexKey() throws Exception {

        ElementIdKeyFunction keyFunction = new ElementIdKeyFunction();

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
