package com.intel.pig.udf.load;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class VertexLoaderTest {

    @Test
    public void testGetNext_NoItems() throws Exception {

        // setup mocks
        RecordReader reader = mock(RecordReader.class);
        when(reader.nextKeyValue()).thenReturn(false);

        // initialize class under test
        VertexLoader vertexLoader = new VertexLoader("false");
        vertexLoader.prepareToRead(reader, null);

        // invoke method under test
        Tuple tuple = vertexLoader.getNext();

        // assertions
        assertNull(tuple);
    }

    @Test
    public void testGetNext_OneItem() throws Exception {

        // setup mocks
        RecordReader reader = mock(RecordReader.class);
        when(reader.nextKeyValue()).thenReturn(true);
        when(reader.getCurrentValue()).thenReturn(new Text("name\tlabel"));

        // initialize class under test
        VertexLoader vertexLoader = new VertexLoader("false");
        vertexLoader.prepareToRead(reader, null);

        // invoke method under test
        Tuple tuple = vertexLoader.getNext();

        // assertions
        assertEquals(1, tuple.size());
        SerializedGraphElement serializedGraphElement = (SerializedGraphElement) tuple.get(0);
        Vertex vertex = (Vertex) serializedGraphElement.graphElement();
        assertEquals("name", vertex.getId().getName().toString());
        assertEquals("label", vertex.getLabel().toString());
    }

    @Test
    public void testGetNext_OneItemWithDelimiter() throws Exception {

        // setup mocks
        RecordReader reader = mock(RecordReader.class);
        when(reader.nextKeyValue()).thenReturn(true);
        when(reader.getCurrentValue()).thenReturn(new Text("name,label"));

        // initialize class under test
        VertexLoader vertexLoader = new VertexLoader("false", ",");
        vertexLoader.prepareToRead(reader, null);

        // invoke method under test
        Tuple tuple = vertexLoader.getNext();

        // assertions
        assertEquals(1, tuple.size());
        SerializedGraphElement serializedGraphElement = (SerializedGraphElement) tuple.get(0);
        Vertex vertex = (Vertex) serializedGraphElement.graphElement();
        assertEquals("name", vertex.getId().getName().toString());
        assertEquals("label", vertex.getLabel().toString());
    }
}
