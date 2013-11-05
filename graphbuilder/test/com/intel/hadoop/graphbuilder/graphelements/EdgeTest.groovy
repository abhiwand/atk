package com.intel.hadoop.graphbuilder.graphelements

import com.intel.hadoop.graphbuilder.types.IntType
import com.intel.hadoop.graphbuilder.types.PropertyMap
import com.intel.hadoop.graphbuilder.types.StringType
import org.apache.hadoop.io.IntWritable
import org.junit.Test

import static junit.framework.Assert.assertFalse
import static junit.framework.Assert.assertNotNull
import static junit.framework.Assert.assertNotNull
import static junit.framework.Assert.assertNull
import static junit.framework.Assert.assertSame


public class EdgeTest {

    @Test
    public final void testConstructorWithoutArgs() {
        Edge<StringType> edge = new Edge<StringType>()

        assertNotNull(edge)
        assertNull(edge.getSrc())
        assertNull(edge.getDst())
        assertNull(edge.getEdgeLabel())
        assertNotNull(edge.getEdgeID())
        assertNotNull(edge.getProperties())
    }

    @Test
    public final void testConstructorWithArgs() {
        StringType src   = new StringType("src")
        StringType dst   = new StringType("dst")
        StringType label = new StringType("label")

        Edge<StringType> edge = new Edge<StringType>(src, dst, label)

        assertSame(src, edge.getSrc())
        assertSame(dst, edge.getDst())
        assertSame(label, edge.getEdgeLabel())
        assertNotNull(edge.getEdgeID())
        assertNotNull(edge.getProperties())
    }

    @Test
    public final void testConfigure() {
        StringType src   = new StringType("src")
        StringType dst   = new StringType("dst")
        StringType label = new StringType("label")
        PropertyMap pm   = new PropertyMap()

        Edge<StringType> edge = new Edge<StringType>()

        edge.configure(src, dst, label, pm)

        assertSame(src, edge.getSrc())
        assertSame(dst, edge.getDst())
        assertSame(label, edge.getEdgeLabel())
        assertSame(pm, edge.getProperties())

        // now test against an edge created with arguments in the constructor

        StringType src2   = new StringType("src2")
        StringType dst2  = new StringType("dst2")
        StringType label2 = new StringType("label2")

        Edge<StringType> edge2 = new Edge<StringType>(src2,dst2,label2)

        edge2.configure(src, dst, label, pm)

        assertSame(src, edge2.getSrc())
        assertSame(dst, edge2.getDst())
        assertSame(label, edge2.getEdgeLabel())
        assertSame(pm, edge2.getProperties())
    }

    @Test
    public final void testProperties() {
        StringType  src   = new StringType("src")
        StringType  dst   = new StringType("dst")
        StringType  label = new StringType("label")

        String key1  = new String("key")
        String key2  = new String("Ce n'est pas une clé")

        StringType value1 = new StringType("Outstanding Value")
        StringType value2 = new StringType("Little Value")

        Edge<StringType> edge = new Edge<StringType>(src, dst, label)

        assert(edge.getProperties().getPropertyKeys().empty)

        edge.setProperty(key1,value1)
        edge.setProperty(key2, value2)

        assertSame(value1, edge.getProperty(key1))
        assertSame(value1, edge.getProperties().getProperty(key1))

        assertSame(value2, edge.getProperty(key2))
        assertSame(value2, edge.getProperties().getProperty(key2))

        edge.setProperty(key1,value2)
        edge.setProperty(key2, value1)

        assertSame(value2, edge.getProperty(key1))
        assertSame(value2, edge.getProperties().getProperty(key1))

        assertSame(value1, edge.getProperty(key2))
        assertSame(value1, edge.getProperties().getProperty(key2))

        assert(edge.getProperties().getPropertyKeys().size() == 2)



    }

    @Test
    public final void testGetEdgeID() {
        StringType src1   = new StringType("src1")
        StringType dst1   = new StringType("dst1")
        StringType label1 = new StringType("label1")

        StringType src2   = new StringType("src2")
        StringType dst2   = new StringType("dst2")
        StringType label2 = new StringType("label2")


        Edge<StringType> edge1 = new Edge<>(src1, dst1, label1)
        Edge<StringType> edge2 = new Edge<>(src2, dst2, label2)
        Edge<StringType> edge3 = new Edge<>(src1, dst1, label1)


        assertNotNull(edge1.getEdgeID())
        assertNotNull(edge2.getEdgeID())
        assertNotNull(edge3.getEdgeID())

        assertFalse(edge1.getEdgeID().equals(edge2.getEdgeID()))
        assert(edge1.getEdgeID().equals(edge3.getEdgeID()))

        String     key   = new String("key")
        StringType value = new StringType("bank")

        edge1.setProperty(key, value)
        assertFalse(edge1.getEdgeID().equals(edge2.getEdgeID()))
        assert(edge1.getEdgeID().equals(edge3.getEdgeID()))

    }

    @Test
    public final void testToString() {
        StringType src1   = new StringType("src1")
        StringType dst1   = new StringType("dst1")
        StringType label1 = new StringType("label1")

        StringType src2   = new StringType("src2")
        StringType dst2   = new StringType("dst2")
        StringType label2 = new StringType("label2")


        Edge<StringType> edge1 = new Edge<>(src1, dst1, label1)
        Edge<StringType> edge2 = new Edge<>(src2, dst2, label2)
        Edge<StringType> edge3 = new Edge<>(src1, dst1, label1)


        assertNotNull(edge1.toString())
        assertNotNull(edge2.toString())
        assertNotNull(edge3.toString())

        assertFalse(edge1.toString().equals(edge2.toString()))
        assert(edge1.toString().equals(edge3.toString()))

        String     key   = new String("key")
        StringType value = new StringType("bank")

        edge1.setProperty(key, value)
        assertFalse(edge1.toString().equals(edge2.toString()))
        assertFalse(edge1.toString().equals(edge3.toString()))

    }

    @Test
    public final void testSelfEdge() {
        StringType  src   = new StringType("src")
        StringType  dst   = new StringType("dst")
        StringType  label = new StringType("label")

        Edge<StringType> nonLoop = new Edge<StringType>(src,dst,label)
        Edge<StringType> loop    = new Edge<StringType>(src, src, label)

        assertFalse(nonLoop.isSelfEdge())
        assert(loop.isSelfEdge())

    }

    @Test
    public final void testWriteRead() {
        StringType  src   = new StringType("src")
        StringType  dst   = new StringType("dst")
        StringType  label = new StringType("label")

        Edge<StringType> edge = new Edge<StringType>(src,dst,label)

        StringType src2   = new StringType("src2")
        StringType dst2  = new StringType("dst2")
        StringType label2 = new StringType("label2")
        Edge<StringType> edgeOnTheOtherEnd = new Edge<StringType>(src2, dst2, label2)

        ByteArrayOutputStream baos             = new ByteArrayOutputStream(1024)
        DataOutputStream      dataOutputStream = new DataOutputStream(baos)

        edge.write(dataOutputStream)
        dataOutputStream.flush()

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray())
        DataInputStream dataInputStream = new DataInputStream(bais)


        edgeOnTheOtherEnd.readFields(dataInputStream)

        assert(edgeOnTheOtherEnd.getEdgeID().equals(edge.getEdgeID()))
        assert(edgeOnTheOtherEnd.getProperties().toString().equals(edge.getProperties().toString()))

        // one more time, with a nonempty property list

        String     key   = new String("key")
        StringType value = new StringType("bank")

        edge.setProperty(key, value)


        ByteArrayOutputStream baos2             = new ByteArrayOutputStream(1024)
        DataOutputStream      dataOutputStream2 = new DataOutputStream(baos2)

        edge.write(dataOutputStream2)
        dataOutputStream.flush()

        ByteArrayInputStream bais2 = new ByteArrayInputStream(baos2.toByteArray())
        DataInputStream dataInputStream2 = new DataInputStream(bais2)



        edgeOnTheOtherEnd.readFields(dataInputStream2)

        assert(edgeOnTheOtherEnd.getEdgeID().equals(edge.getEdgeID()))
        assert(edgeOnTheOtherEnd.getProperties().toString().equals(edge.getProperties().toString()))

    }
}
