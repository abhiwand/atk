package com.intel.hadoop.graphbuilder.types;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.util.Set;

import static junit.framework.Assert.assertEquals;

public class PropertyMapTest {
    @Test
    public void testSetProperty() throws Exception {
        PropertyMap pm= new PropertyMap();

        IntWritable    one = new IntWritable(1);
        IntWritable    two = new IntWritable(2);
        DoubleWritable pi  = new DoubleWritable(3.14159);

        pm.setProperty("p1", one);
        pm.setProperty("p2", pi);
        pm.setProperty("p1", two);

        // assert for object equality, the maps should not replace objects
        assertEquals(pm.getProperty("p1"), two);
        assertEquals(pm.getProperty("p2"), pi);
    }

    @Test
    public void testRemoveProperty() throws Exception {
        PropertyMap pm= new PropertyMap();

        IntWritable    one = new IntWritable(1);
        DoubleWritable pi  = new DoubleWritable(3.14159);

        pm.setProperty("p1", one);
        pm.setProperty("p2", pi);

        assertEquals(pm.getProperty("p2"), pi);

        pm.removeProperty("p2");

        assertEquals(pm.getProperty("p1"), one);
        assertEquals(pm.getProperty("p2"), null);
    }

    @Test
    public void testGetProperty() throws Exception {
        PropertyMap pm= new PropertyMap();

        assertEquals(pm.getProperty("foo"), null);



        IntWritable    one = new IntWritable(1);
        IntWritable    two = new IntWritable(2);
        DoubleWritable pi  = new DoubleWritable(3.14159);

        pm.setProperty("foo", one);

        // assert for object equality, the maps should not replace objects
        assertEquals(pm.getProperty("foo"), one);
        assertEquals(pm.getProperty("foo"), one);
    }

    @Test
    public void testGetPropertyKeys() throws Exception {
        PropertyMap pm= new PropertyMap();

        assert(pm.getPropertyKeys().isEmpty());

        IntWritable    one = new IntWritable(1);
        IntWritable    two = new IntWritable(2);
        DoubleWritable pi  = new DoubleWritable(3.14159);

        pm.setProperty("p1", one);
        pm.setProperty("p2", pi);
        pm.setProperty("p1", two);

        Set<Writable> keySet = pm.getPropertyKeys();

        for (Writable key : keySet)
        {
            assert(key.toString().compareTo("p1") == 0 || key.toString().compareTo("p2") == 0);
        }

        boolean foundP1 = false;
        boolean foundP2 = false;

        for (Writable key : keySet)
        {
            foundP1 |= (key.toString().compareTo("p1") == 0);
            foundP2 |= (key.toString().compareTo("p2") == 0);
        }

        assert(foundP1);
        assert(foundP2);
    }

}
