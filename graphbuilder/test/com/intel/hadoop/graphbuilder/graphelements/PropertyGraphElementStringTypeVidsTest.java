package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class PropertyGraphElementStringTypeVidsTest {
    @Test
    public void testCreateVid() {
        PropertyGraphElementStringTypeVids elt = new PropertyGraphElementStringTypeVids();

        assertNotNull(elt);

        Object vid = elt.createVid();

        assertNotNull(vid);

        assertEquals(vid.getClass(), StringType.class);
    }

    @Test
    public void testToString() {

        // expecting that the vertex ID show up in the string representation of a vertex property graph element
        // and that the source ID, destination ID and label show up in the string representation of an edge property
        // graph element seems like a reasonable expectation

        String     name = "veni VID-i vici";
        StringType vid  = new StringType(name);

        Vertex<StringType> vertex = new Vertex<StringType>(vid);

        PropertyGraphElementStringTypeVids vertexElement  = new PropertyGraphElementStringTypeVids();

        vertexElement.init(PropertyGraphElement.GraphElementType.VERTEX, vertex);

        assert(vertexElement.toString().contains(name));


        String srcName = "The Source";
        String dstName = "Destination Unkown";

        StringType srcId = new StringType(srcName);
        StringType dstId = new StringType(dstName);

        String label   = "no labels, please";
        StringType wrappedLabel = new StringType(label);

        Edge<StringType> edge = new Edge<StringType>(srcId, dstId, wrappedLabel);

        PropertyGraphElementStringTypeVids edgeElement  = new PropertyGraphElementStringTypeVids();

        edgeElement.init(PropertyGraphElement.GraphElementType.EDGE, edge);

        assert(edgeElement.toString().contains(srcName));
        assert(edgeElement.toString().contains(dstName));
        assert(edgeElement.toString().contains(label));

        // as for the null graph element...
        // well, I don't care what you call it, but it needs to have nonzero length string

        PropertyGraphElementStringTypeVids nullElement = new PropertyGraphElementStringTypeVids();

        assert(nullElement.toString().length() > 0);
    }
}
