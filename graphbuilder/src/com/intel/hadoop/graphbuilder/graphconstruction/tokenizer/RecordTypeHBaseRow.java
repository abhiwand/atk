

package com.intel.hadoop.graphbuilder.graphconstruction.tokenizer;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * RecordTypeHBaseRow is a way to treat the hbase reader output as one data object, which is what the parser expects,
 * as opposed to two objects, which is what the hbase reader returns
 */
public class RecordTypeHBaseRow {

    private ImmutableBytesWritable row;
    private Result                 columns;

    public RecordTypeHBaseRow(ImmutableBytesWritable row, Result columns) {
        this.row     = row;
        this.columns = columns;
    }

    public void setRow(ImmutableBytesWritable row) {
        this.row = row;
    }

    public ImmutableBytesWritable getRow() {
        return this.row;
    }

    public void setColumns(Result columns) {
        this.columns = columns;
    }

    public Result getColumns() {
        return this.columns;
    }
}
