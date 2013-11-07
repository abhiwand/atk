

package com.intel.hadoop.graphbuilder.graphconstruction.tokenizer;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Encapsulates hbase reader output as one data object.
 *
 * It has two parts:
 * <ul>
 *     <li>{@code ImmutableBytesWritable row }  the hbase row</li>
 *     <li>{@code Result columns} the columns of the row</li>
 * </ul>
 * @see com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.HBaseReaderMapper
 */
public class RecordTypeHBaseRow {

    private ImmutableBytesWritable row;
    private Result                 columns;

    /**
     * Constructor that takes a row and its columns.
     * @param row
     * @param columns
     */
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
