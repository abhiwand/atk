package com.intel.etl;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/*

 */
public class TestColumnDropper {
    @Test
    public void testSplitMultipleColumnNames() {
        String colNames = "src,weight, dest";
        List<String> fields = HBaseColumnDropperMapper.splitFields(colNames);
        assertEquals("should have 3 columns", 3, fields.size());
        assertEquals("first column", "src", fields.get(0));
        assertEquals("second column", "weight", fields.get(1));
        assertEquals("third column", "dest", fields.get(2));

    }
}
