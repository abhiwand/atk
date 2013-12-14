package com.intel.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;


public class HBaseColumnDropperMapper extends TableMapper<IntWritable, IntWritable> {

    HTable table;
    @Override
    public void setup(Context context) {
        try {
            table = new HTable(context.getConfiguration(), "test_output_2");
        } catch(Exception e) {

        }
    }

    @Override
    public void map(ImmutableBytesWritable row, Result columns, Context context) {

        Configuration conf = context.getConfiguration();
        String columnFamily = conf.get(HBaseColumnDropper.COLUMN_FAMILY);
        String columnName = conf.get(HBaseColumnDropper.COLUMN_NAME);

        Delete delete = new Delete(row.get());
        System.out.println(columnFamily);
        System.out.println(columnName);
        System.out.println("start...");

        delete.deleteColumn(columnFamily.getBytes(), columnName.getBytes());
        try {
            table.delete(delete);
        } catch(Exception e) {

        }
        System.out.println("end...");
    }
}

