package com.intel.hadoop.graphbuilder.graphconstruction;

import com.intel.hadoop.graphbuilder.util.Triple;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;

public class EdgeKeyTest {
    @Test
    public final void testGetA() throws Exception {
        Triple<Integer, Integer, Integer> t = new Triple(7, 8, 9);

        int a = t.getA();
        assertEquals(a, 7);
    }

    @Test
    public final void testConstructorGet() {
        Object src = new String("src");
        Object dst = new String("dst");
        Object label = new String("label");

        EdgeKey edgeKey = new EdgeKey(src, dst, label);

        assertNotNull(edgeKey);
        assertSame(edgeKey.getSrc(), src);
        assertSame(edgeKey.getDst(), dst);
        assertSame(edgeKey.getLabel(), label);
    }

    @Test
    public final void testConstructorGetSet() {
        Object src = new String("src");
        Object dst = new String("dst");
        Object label = new String("label");

        Object one = new Integer(0);
        Object two = new Integer(1);
        Object three = new Integer(2);

        EdgeKey edgeKey = new EdgeKey(src, dst, label);

        assertNotNull(edgeKey);
        assertSame(edgeKey.getSrc(), src);
        assertSame(edgeKey.getDst(), dst);
        assertSame(edgeKey.getLabel(), label);

        edgeKey.setSrc(one);

        assertSame(edgeKey.getSrc(), one);
        assertSame(edgeKey.getDst(), dst);
        assertSame(edgeKey.getLabel(), label);

        edgeKey.setDst(two);

        assertSame(edgeKey.getSrc(), one);
        assertSame(edgeKey.getDst(), two);
        assertSame(edgeKey.getLabel(), label);

        edgeKey.setLabel(three);

        assertSame(edgeKey.getSrc(), one);
        assertSame(edgeKey.getDst(), two);
        assertSame(edgeKey.getLabel(), three);

        edgeKey.setLabel(label);

        assertSame(edgeKey.getSrc(), one);
        assertSame(edgeKey.getDst(), two);
        assertSame(edgeKey.getLabel(), label);

        edgeKey.setDst(dst);

        assertSame(edgeKey.getSrc(), one);
        assertSame(edgeKey.getDst(), dst);
        assertSame(edgeKey.getLabel(), label);

        edgeKey.setSrc(src);

        assertSame(edgeKey.getSrc(), src);
        assertSame(edgeKey.getDst(), dst);
        assertSame(edgeKey.getLabel(), label);
    }

    @Test
    public final void testReverseEdge() {
        Object src = new String("src");
        Object dst = new String("dst");
        Object label = new String("label");

        EdgeKey edgeKey = new EdgeKey(src, dst, label);

        assertNotNull(edgeKey);
        assertSame(edgeKey.getSrc(), src);
        assertSame(edgeKey.getDst(), dst);
        assertSame(edgeKey.getLabel(), label);

        EdgeKey reverseEdgeKey = edgeKey.reverseEdge();
        assertSame(reverseEdgeKey.getSrc(), dst);
        assertSame(reverseEdgeKey.getDst(), src);
        assertSame(reverseEdgeKey.getLabel(), label);

        EdgeKey reverseReverseEdgeKey = edgeKey.reverseEdge().reverseEdge();
        assertSame(reverseReverseEdgeKey.getSrc(), src);
        assertSame(reverseReverseEdgeKey.getDst(), dst);
        assertSame(reverseReverseEdgeKey.getLabel(), label);
    }

    @Test
    public final void testEquals() {
        Object src = new String("src");
        Object badSrc = new String("badSrc");
        Object dst = new String("dst");
        Object badDst = new String("badDst");
        Object label = new String("label");
        Object badLabel = new String("I hate labels");

        EdgeKey edgeKey = new EdgeKey(src, dst, label);
        EdgeKey edgeKeySame = new EdgeKey(src, dst, label);
        EdgeKey edgeKeyDelta1 = new EdgeKey(badSrc, dst, label);
        EdgeKey edgeKeyDelta2 = new EdgeKey(src, badDst, label);
        EdgeKey edgeKeyDelta3 = new EdgeKey(src, dst, badLabel);

        assert (edgeKey.equals(edgeKeySame));
        assertFalse(edgeKey.equals(edgeKeyDelta1));
        assertFalse(edgeKey.equals(edgeKeyDelta2));
        assertFalse(edgeKey.equals(edgeKeyDelta3));

        // can't forget this one
        assertFalse(edgeKey.equals(null));
    }

    @Test
    public final void testHashCode() {
        Object src = new String("src");
        Object dst = new String("dst");
        Object label = new String("label");

        EdgeKey edgeKey = new EdgeKey(src, dst, label);

        int hash = edgeKey.hashCode();

        assertNotNull(hash);
    }

    @Test
    public final void testToString() {
        Object src = new String("src");
        Object dst = new String("dst");
        Object label = new String("label");

        Object src2 = new String("src");
        Object dst2 = new String("dst");
        Object label2 = new String("label");

        EdgeKey edgeKey1 = new EdgeKey(src, dst, label);
        EdgeKey edgeKey2 = new EdgeKey(src2, dst2, label2);

        String toString1 = edgeKey1.toString();
        String toString2 = edgeKey2.toString();

        assertNotNull(toString1);
        assertFalse(toString1.compareTo("") == 0);

        assert (toString2.compareTo(toString1) == 0);

        // all those wonderful negative tests

        Object badSrc = new String("badSrc");
        Object badDst = new String("badDst");
        Object badLabel = new String("badLabel");

        EdgeKey edgeKeyDelta1 = new EdgeKey(badSrc, dst, label);
        EdgeKey edgeKeyDelta2 = new EdgeKey(src, badDst, label);
        EdgeKey edgeKeyDelta3 = new EdgeKey(src, dst, badLabel);

        assertFalse(toString1.compareTo(edgeKeyDelta1.toString()) == 0);
        assertFalse(toString1.compareTo(edgeKeyDelta2.toString()) == 0);
        assertFalse(toString1.compareTo(edgeKeyDelta3.toString()) == 0);
    }
}
