package com.intel.graphbuilder.io;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;


public class HBaseTableInputFormatTest {

    public HBaseTableInputFormat tableInputFormat = null;

    @Before
    public void setUp() throws Exception {
        this.tableInputFormat = new HBaseTableInputFormat();

    }

    @After
    public void tearDown() throws Exception {
      this.tableInputFormat = null;
    }

    @Test
    public void testGetSplits() throws Exception {
        byte [] lowest = Bytes.toBytes("AAA");
        byte [] middle = Bytes.toBytes("CCC");
        byte [] highest = Bytes.toBytes("EEE");


        byte [][] parts = Bytes.split(lowest, highest, 1);
        for (int i = 0; i < parts.length; i++) {
            System.out.println(Bytes.toString(parts[i]));
        }
        assertEquals(3, parts.length);
        assertTrue(Bytes.equals(parts[1], middle));
        // Now divide into three parts.  Change highest so split is even.
        highest = Bytes.toBytes("DDD");
        parts = Bytes.split(lowest, highest, 2);
        for (int i = 0; i < parts.length; i++) {
            System.out.println(Bytes.toString(parts[i]));
        }
        assertEquals(4, parts.length);
        // Assert that 3rd part is 'CCC'.
        assertTrue(Bytes.equals(parts[2], middle));

    }


}