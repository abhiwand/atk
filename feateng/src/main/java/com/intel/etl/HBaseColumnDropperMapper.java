package com.intel.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;


public class HBaseColumnDropperMapper extends TableMapper<IntWritable, IntWritable> {

    HTable table;
    @Override
    public void setup(Context context) {

        Configuration conf = context.getConfiguration();
        String tableName = conf.get(HBaseColumnDropper.TABLE_NAME);
        try {
            table = new HTable(context.getConfiguration(), tableName);
        } catch(IOException e) {

        }
    }

    @Override
    public void map(ImmutableBytesWritable row, Result columns, Context context) {

        Configuration conf = context.getConfiguration();
        String columnFamily = conf.get(HBaseColumnDropper.COLUMN_FAMILY);
        String columnName = conf.get(HBaseColumnDropper.COLUMN_NAME);

        Delete delete = new Delete(row.get());
        delete.deleteColumn(columnFamily.getBytes(), columnName.getBytes());
        try {
            table.delete(delete);
        } catch(IOException e) {

        }
    }
}

