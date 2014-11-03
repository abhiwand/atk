package com.intel.graphbuilder.io;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.JobContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HBaseTableInputFormatTest {



    @Test
    public void testGetSplits() throws Exception {
        JobContext context = mock(JobContext.class);
        HBaseTableInputFormat tableInputFormat = mock(HBaseTableInputFormat.class);

        byte[] startRow = Bytes.toBytes("AAA");
        byte[] middle = Bytes.toBytes("CCC");
        byte[] endRow = Bytes.toBytes("EEE");

        TableSplit tableSplit1 = new TableSplit(TableName.valueOf("table"), startRow, endRow, "location");
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(tableSplit1);
        when(tableInputFormat.getInitialRegionSplits(context)).thenReturn(inputSplits);
        when(tableInputFormat.getRequestedSplitCount(context, inputSplits)).thenReturn(2);

        // Test split into 2 parts
        List<InputSplit> uniformSplits = tableInputFormat.getSplits(context);

        /*assertEquals(2, uniformSplits.size());
        // AAA to CCC
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getStartRow(), startRow));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getEndRow(), middle));
        // CCC to EEE
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getStartRow(), middle));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getEndRow(), endRow));

          */
    }


}