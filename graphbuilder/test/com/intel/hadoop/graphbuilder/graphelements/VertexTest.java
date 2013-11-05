package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import java.io.*;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertFalse;

public class VertexTest {

    @Test
    public final void testNoArgConstructor() {
        Vertex<StringType> vertex = new Vertex<StringType>();

        assertNotNull(vertex);
        assertNull(vertex.getVertexId());
        assertNotNull(vertex.getProperties());
    }

    @Test
    public final void testConstructorWithArgs() {
        StringType vertexId = new StringType("The Greatest Vertex EVER");
        Vertex<StringType> vertex = new Vertex<StringType>(vertexId);

        assertNotNull(vertex);
        assert (vertex.getVertexId().equals(vertexId));
        assertNotNull(vertex.getProperties());
    }

    @Test
    public final void testConfigureWithGetters() {

        StringType vertexId = new StringType("The Greatest Vertex EVER");
        Vertex<StringType> vertex = new Vertex<StringType>(vertexId);

        assertNotNull(vertex);
        assert (vertex.getVertexId().equals(vertexId));
        assertNotNull(vertex.getProperties());

        PropertyMap pm = vertex.getProperties();
        PropertyMap pm2 = new PropertyMap();

        StringType anotherOpinion = new StringType("No that vertex sucks");

        vertex.configure(anotherOpinion, pm2);
        assert (vertex.getVertexId().equals(anotherOpinion));
        assertSame(vertex.getProperties(), pm2);

        vertex.configure(vertexId, pm);
        assert (vertex.getVertexId().equals(vertexId));
        assertSame(vertex.getProperties(), pm);
    }

    @Test
    public final void testGetSetProperty() {
        StringType vertexId = new StringType("The Greatest Vertex EVER");
        Vertex<StringType> vertex = new Vertex<StringType>(vertexId);

        String key1 = new String("key");
        String key2 = new String("Ce n'est pas une clé");

        StringType value1 = new StringType("Outstanding Value");
        StringType value2 = new StringType("Little Value");

        assert (vertex.getProperties().getPropertyKeys().isEmpty());

        vertex.setProperty(key1, value1);
        assertSame(vertex.getProperty(key1), value1);
        assertNull(vertex.getProperty(key2));

        vertex.setProperty(key2, value2);
        assertSame(vertex.getProperty(key1), value1);
        assertSame(vertex.getProperty(key2), value2);

        vertex.setProperty(key1, value2);
        assertSame(vertex.getProperty(key1), value2);
        assertSame(vertex.getProperty(key2), value2);

        vertex.setProperty(key2, value1);
        assertSame(vertex.getProperty(key1), value2);
        assertSame(vertex.getProperty(key2), value1);

        assert (vertex.getProperties().getPropertyKeys().size() == 2);
    }

    @Test
    public final void testToString() {
        StringType id1 = new StringType("the greatest vertex ID ever");
        StringType id2 = new StringType("worst vertex ID ever");

        Vertex<StringType> vertex1 = new Vertex<StringType>(id1);
        Vertex<StringType> vertex2 = new Vertex<StringType>(id2);

        Vertex<StringType> vertex3 = new Vertex<StringType>(id1);

        assertNotNull(vertex1.toString());
        assertNotNull(vertex2.toString());
        assertNotNull(vertex3.toString());

        assert (vertex1.toString().compareTo(vertex2.toString()) != 0);
        assert (vertex1.toString().compareTo(vertex3.toString()) == 0);

        String key = new String("key");
        StringType value = new StringType("bank");

        vertex1.setProperty(key, value);

        assert (vertex1.toString().compareTo(vertex2.toString()) != 0);
        assert (vertex1.toString().compareTo(vertex3.toString()) != 0);
    }

    @Test
    public final void testWriteRead() throws IOException {
        StringType id = new StringType("the greatest vertex of ALLL TIIIME!!!");
        Vertex<StringType> vertex = new Vertex<StringType>(id);

        Vertex<StringType> vertexOnTheOtherEnd = new Vertex<StringType>(new StringType("maybe not so much"));

        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream dataOutputStream = new DataOutputStream(baos);

        vertex.write(dataOutputStream);
        dataOutputStream.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(bais);

        vertexOnTheOtherEnd.readFields(dataInputStream);

        assert (vertex.getVertexId().equals(vertexOnTheOtherEnd.getVertexId()));
        assert (vertex.getProperties().toString().equals(vertexOnTheOtherEnd.getProperties().toString()));

        // one more time, with a nonempty property list

        String key = new String("key");
        IntType value = new IntType(666);

        vertex.setProperty(key, value);

        ByteArrayOutputStream baos2 = new ByteArrayOutputStream(1024);
        DataOutputStream dataOutputStream2 = new DataOutputStream(baos2);

        vertex.write(dataOutputStream2);
        dataOutputStream.flush();

        ByteArrayInputStream bais2 = new ByteArrayInputStream(baos2.toByteArray());
        DataInputStream dataInputStream2 = new DataInputStream(bais2);

        vertexOnTheOtherEnd.readFields(dataInputStream2);

        assert (vertex.getVertexId().equals(vertexOnTheOtherEnd.getVertexId()));
        assert (vertex.getProperties().toString().equals(vertexOnTheOtherEnd.getProperties().toString()));
    }
}
