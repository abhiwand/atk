package com.intel.pig.udf.load;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EdgeLoaderTest {

    @Test
    public void testGetNext_NoItems() throws Exception {

        // setup mocks
        RecordReader reader = mock(RecordReader.class);
        when(reader.nextKeyValue()).thenReturn(false);

        // initialize class under test
        EdgeLoader edgeLoader = new EdgeLoader("false");
        edgeLoader.prepareToRead(reader, null);

        // invoke method under test
        Tuple tuple = edgeLoader.getNext();

        // assertions
        assertNull(tuple);
    }

    @Test
    public void testGetNext_OneItem() throws Exception {

        // setup mocks
        RecordReader reader = mock(RecordReader.class);
        when(reader.nextKeyValue()).thenReturn(true);
        when(reader.getCurrentValue()).thenReturn(new Text("vertexLabel1.0001\tvertexLabel2.0002\tedgeLabel"));

        // initialize class under test
        EdgeLoader edgeLoader = new EdgeLoader("false");
        edgeLoader.prepareToRead(reader, null);

        // invoke method under test
        Tuple tuple = edgeLoader.getNext();

        // assertions
        assertEquals(1, tuple.size());
        SerializedGraphElement serializedGraphElement = (SerializedGraphElement) tuple.get(0);
        Edge edge = (Edge) serializedGraphElement.graphElement();
        assertEquals("0001", edge.getSrc().getName().toString());
        assertEquals("0002", edge.getDst().getName().toString());
        assertEquals("vertexLabel1", edge.getSrc().getLabel().toString());
        assertEquals("vertexLabel2", edge.getDst().getLabel().toString());
        assertEquals("edgeLabel", edge.getLabel().toString());
    }

    @Test
    public void testGetNext_OneItemWithDelimiter() throws Exception {

        // setup mocks
        RecordReader reader = mock(RecordReader.class);
        when(reader.nextKeyValue()).thenReturn(true);
        when(reader.getCurrentValue()).thenReturn(new Text("vertexLabel1.0001,vertexLabel2.0002,edgeLabel"));

        // initialize class under test
        EdgeLoader edgeLoader = new EdgeLoader("false", ",");
        edgeLoader.prepareToRead(reader, null);

        // invoke method under test
        Tuple tuple = edgeLoader.getNext();

        // assertions
        assertEquals(1, tuple.size());
        SerializedGraphElement serializedGraphElement = (SerializedGraphElement) tuple.get(0);
        Edge edge = (Edge) serializedGraphElement.graphElement();
        assertEquals("0001", edge.getSrc().getName().toString());
        assertEquals("0002", edge.getDst().getName().toString());
        assertEquals("vertexLabel1", edge.getSrc().getLabel().toString());
        assertEquals("vertexLabel2", edge.getDst().getLabel().toString());
        assertEquals("edgeLabel", edge.getLabel().toString());
    }
}
