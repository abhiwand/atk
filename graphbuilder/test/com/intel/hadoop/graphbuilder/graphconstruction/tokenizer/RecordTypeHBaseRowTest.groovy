package com.intel.hadoop.graphbuilder.graphconstruction.tokenizer

import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.EdgeSchema
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.junit.Test

import static junit.framework.Assert.assertNotNull
import static junit.framework.Assert.assertSame

class RecordTypeHBaseRowTest {

    @Test
    void testConstructorGet() {
        ImmutableBytesWritable row     = new ImmutableBytesWritable();
        Result                 columns = new Result();
        RecordTypeHBaseRow     record  = new RecordTypeHBaseRow(row, columns)

        assertNotNull(record)
        assertSame(row, record.getRow())
        assertSame(columns, record.getColumns())
    }

    @Test
    void testSetGetRow() {
        ImmutableBytesWritable row1     = new ImmutableBytesWritable();
        Result                 columns  = new Result();
        ImmutableBytesWritable row2     = new ImmutableBytesWritable();
        ImmutableBytesWritable row3     = new ImmutableBytesWritable();

        RecordTypeHBaseRow     record  = new RecordTypeHBaseRow(row1, columns)

        assertSame(row1, record.getRow())
        assertSame(columns, record.getColumns())

        record.setRow(row2)
        assertSame(row2, record.getRow())
        assertSame(columns, record.getColumns())

        record.setRow(row3)
        assertSame(row3, record.getRow())
        assertSame(columns, record.getColumns())

    }

    @Test
    void testSetGetColumns() {
        ImmutableBytesWritable row      = new ImmutableBytesWritable();
        Result                 columns1 = new Result();
        Result                 columns2 = new Result();
        Result                 columns3 = new Result();

        RecordTypeHBaseRow     record  = new RecordTypeHBaseRow(row, columns1)

        assertSame(row, record.getRow())
        assertSame(columns1, record.getColumns())

        record.setColumns(columns2)
        assertSame(row, record.getRow())
        assertSame(columns2, record.getColumns())

        record.setColumns(columns3)
        assertSame(row, record.getRow())
        assertSame(columns3, record.getColumns())
    }

}
