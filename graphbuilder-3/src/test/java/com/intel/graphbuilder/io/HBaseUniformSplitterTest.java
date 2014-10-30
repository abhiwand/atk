package com.intel.graphbuilder.io;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class HBaseUniformSplitterTest {

    @Test
    public void testCreate2InputSplits() throws Exception {
        byte[] startRow = Bytes.toBytes("AAA");
        byte[] middle = Bytes.toBytes("CCC");
        byte[] endRow = Bytes.toBytes("EEE");

        // Test split into 2 parts
        TableSplit tableSplit1 = new TableSplit(TableName.valueOf("table"), startRow, endRow, "location");
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(tableSplit1);

        HBaseUniformSplitter uniformSplitter = new HBaseUniformSplitter(inputSplits);
        List<InputSplit> uniformSplits = uniformSplitter.createInputSplits(2);

        assertEquals(2, uniformSplits.size());
        // AAA to CCC
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getStartRow(), startRow));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getEndRow(), middle));
        // CCC to EEE
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getStartRow(), middle));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getEndRow(), endRow));
    }


    @Test
    public void testCreateInput4Splits() throws Exception {
        byte[] startRow = Bytes.toBytes("AAA");
        byte[] middle1 = Bytes.toBytes("BBB");
        byte[] middle2 = Bytes.toBytes("CCC");
        byte[] middle3 = Bytes.toBytes("DDD");
        byte[] endRow = Bytes.toBytes("EEE");

        // Test split into 2 pars
        TableSplit tableSplit1 = new TableSplit(TableName.valueOf("table"), startRow, middle2, "location");
        TableSplit tableSplit2 = new TableSplit(TableName.valueOf("table"), middle2, endRow, "location");
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(tableSplit1);
        inputSplits.add(tableSplit2);

        HBaseUniformSplitter uniformSplitter = new HBaseUniformSplitter(inputSplits);
        List<InputSplit> uniformSplits = uniformSplitter.createInputSplits(4);

        // Test split into four parts.
        assertEquals(4, uniformSplits.size());
        // AAA to BBB
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getStartRow(), startRow));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getEndRow(), middle1));
        // BBB to CCC
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getStartRow(), middle1));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getEndRow(), middle2));
        // CCC to DDD
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(2)).getStartRow(), middle2));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(2)).getEndRow(), middle3));
        // DDD to EEE
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(3)).getStartRow(), middle3));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(3)).getEndRow(), endRow));


    }

    @Test
    public void testCreateInputSplitsEmptyEndRow() throws Exception {
        // If endRow is empty (i.e., last region in table), use max key to split region
        byte xAA = (byte) 0xAA;
        byte xFF = (byte) 0xFF;

        byte[] startRow = HConstants.EMPTY_BYTE_ARRAY;
        byte[] middle1 = Bytes.toBytes("UUUUUUUU");
        byte[] middle2 = new byte[]{xAA, xAA, xAA, xAA, xAA, xAA, xAA, xAA};
        byte[] endRow = new byte[]{xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF};

        TableSplit tableSplit1 = new TableSplit(TableName.valueOf("table"), startRow, HConstants.EMPTY_BYTE_ARRAY, "location");
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(tableSplit1);

        HBaseUniformSplitter uniformSplitter = new HBaseUniformSplitter(inputSplits);
        List<InputSplit> uniformSplits = uniformSplitter.createInputSplits(3);

        // Test split into three parts.
        assertEquals(3, uniformSplits.size());
        // Empty to UUUUUUUU
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getStartRow(), startRow));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getEndRow(), middle1));
        // UUUUUUUU to xAAxAAxAAxAAxAAxAAxAAxAA
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getStartRow(), middle1));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getEndRow(), middle2));
        // xAAxAAxAAxAAxAAxAAxAAxAA to xFFxFFxFFxFFxFFxFFxFFxFF
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(2)).getStartRow(), middle2));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(2)).getEndRow(), endRow));

    }

    @Test
    public void testCreateInvalidSplits() throws Exception {
        byte[] startRow = Bytes.toBytes("AAA");
        byte[] middle = Bytes.toBytes("CCC");
        byte[] endRow = Bytes.toBytes("EEE");

        // If endRow > startRow, do not split table (return input splits)
        TableSplit tableSplit1 = new TableSplit(TableName.valueOf("table"), endRow, startRow, "location");
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(tableSplit1);

        HBaseUniformSplitter uniformSplitter = new HBaseUniformSplitter(inputSplits);
        List<InputSplit> uniformSplits = uniformSplitter.createInputSplits(4);

        assertEquals(1, uniformSplits.size());
        assertTrue(inputSplits.equals(uniformSplits));

        // If splits per region == 0, do not split table
        uniformSplits = uniformSplitter.createInputSplits(0);
        assertEquals(1, uniformSplits.size());
        assertTrue(inputSplits.equals(uniformSplits));


        // If splits per region < 0, do not split table
        uniformSplits = uniformSplitter.createInputSplits(-3);
        assertEquals(1, uniformSplits.size());
        assertTrue(inputSplits.equals(uniformSplits));


        // If startrow == endrow, do not split table
        TableSplit tableSplit2 = new TableSplit(TableName.valueOf("table"), startRow, startRow, "location");
        List<InputSplit> inputSplits2 = new ArrayList<>();
        inputSplits2.add(tableSplit2);

        HBaseUniformSplitter uniformSplitter2 = new HBaseUniformSplitter(inputSplits2);
        List<InputSplit> uniformSplits2 = uniformSplitter2.createInputSplits(5);
        assertEquals(1, uniformSplits2.size());
        assertTrue(inputSplits2.equals(uniformSplits2));

    }


}