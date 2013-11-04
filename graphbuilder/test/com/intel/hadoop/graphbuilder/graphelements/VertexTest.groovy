package com.intel.hadoop.graphbuilder.graphelements

import com.intel.hadoop.graphbuilder.types.PropertyMap
import com.intel.hadoop.graphbuilder.types.StringType
import org.junit.Test

import static junit.framework.Assert.assertNotNull
import static junit.framework.Assert.assertNull
import static junit.framework.Assert.assertSame
import static junit.framework.Assert.assertFalse

class VertexTest {

    @Test
    void testNoArgConstructor() {
        Vertex<StringType> vertex = new Vertex<StringType>()

        assertNotNull(vertex)
        assertNull(vertex.getVertexId())
        assertNotNull(vertex.getProperties())

    }

    @Test
    void testConstructorWithArgs() {
        StringType vertexId       = new StringType("The Greatest Vertex EVER")
        Vertex<StringType> vertex = new Vertex<StringType>(vertexId)


        assertNotNull(vertex)
        assert(vertex.getVertexId().equals(vertexId))
        assertNotNull(vertex.getProperties())
    }

    @Test
    void testConfigureWithGetters() {

        StringType         vertexId = new StringType("The Greatest Vertex EVER")
        Vertex<StringType> vertex   = new Vertex<StringType>(vertexId)


        assertNotNull(vertex)
        assert(vertex.getVertexId().equals(vertexId))
        assertNotNull(vertex.getProperties())

        PropertyMap pm  = vertex.getProperties()
        PropertyMap pm2 = new PropertyMap()

        StringType anotherOpinion = new StringType("No that vertex sucks")

        vertex.configure(anotherOpinion, pm2)
        assert(vertex.getVertexId().equals(anotherOpinion))
        assertSame(vertex.getProperties(), pm2)

        vertex.configure(vertexId, pm)
        assert(vertex.getVertexId().equals(vertexId))
        assertSame(vertex.getProperties(), pm)

    }

    @Test
    void testGetSetProperty() {
        StringType         vertexId = new StringType("The Greatest Vertex EVER")
        Vertex<StringType> vertex   = new Vertex<StringType>(vertexId)

        String key1     = new String("key")
        String key2 = new String("Ce n'est pas une clé")

        StringType value1 = new StringType("Outstanding Value")
        StringType value2 = new StringType("Little Value")

        assertNull(vertex.getProperty(key1))
        assertNull(vertex.getProperty(key2))

        vertex.setProperty(key1, value1)
        assertSame(vertex.getProperty(key1), value1)
        assertNull(vertex.getProperty(key2))

        vertex.setProperty(key2, value2)
        assertSame(vertex.getProperty(key1), value1)
        assertSame(vertex.getProperty(key2), value2)

        vertex.setProperty(key1, value2)
        assertSame(vertex.getProperty(key1), value2)
        assertSame(vertex.getProperty(key2), value2)

        vertex.setProperty(key2, value1)
        assertSame(vertex.getProperty(key1), value2)
        assertSame(vertex.getProperty(key2), value1)


    }

    @Test
    void testEquals() {
        StringType         vertexId      = new StringType("The Greatest Vertex EVER")
        StringType         otherVertexId = new StringType("worst vertex ever")
        Vertex<StringType> baseVertex    = new Vertex<StringType>(vertexId)
        Vertex<StringType> sameIdVertex  = new Vertex<StringType>(vertexId)
        Vertex<StringType> otherIdVertex = new Vertex<StringType>(otherVertexId)


        assert(baseVertex.equals(sameIdVertex))
        assertFalse(baseVertex.equals(otherIdVertex))

        String key1 = new String("key")
        String key2 = new String("Ce n'est pas une clé")

        StringType value1 = new StringType("Outstanding Value")
        StringType value2 = new StringType("Little Value")


    }

    @Test
    void testHashCode() {
        StringType vertexId = new StringType("The Greatest Vertex EVER")
        Vertex<StringType> vertex = new Vertex<StringType>(vertexId)

        int hash1 = vertex.hashCode()

        assertNotNull(hash1)

        Vertex<StringType> vertexNoArgs = new Vertex<StringType>()

        int hash2 = vertexNoArgs.hashCode()

        assertNotNull(hash2)
    }

    @Test
    void testToString() {
        StringType id1 = new StringType("the greatest vertex ID ever")
        StringType id2 = new StringType("worst vertex ID ever")

        Vertex<StringType> vertex1 = new Vertex<StringType>(id1)
        Vertex<StringType> vertex2 = new Vertex<StringType>(id2)

        Vertex<StringType> vertex3 = new Vertex<StringType>(id1)

        assertNotNull(vertex1.toString())
        assertNotNull(vertex2.toString())
        assertNotNull(vertex3.toString())

        assert(vertex1.toString().compareTo(vertex2.toString()) != 0)
        assert(vertex1.toString().compareTo(vertex3.toString()) == 0)
    }
}
