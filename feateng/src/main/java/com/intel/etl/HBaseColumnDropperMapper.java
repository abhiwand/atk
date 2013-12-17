package com.intel.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;


public class HBaseColumnDropperMapper extends TableMapper<IntWritable, IntWritable> {

    byte[] columnFamilyBytes;
    byte[] columnNameBytes;
    List<Delete> deleteList = new ArrayList<Delete>();

    @Override
    public void setup(Context context) {

        Configuration conf = context.getConfiguration();
        columnFamilyBytes = conf.get(HBaseColumnDropper.COLUMN_FAMILY).getBytes();
        columnNameBytes = conf.get(HBaseColumnDropper.COLUMN_NAME).getBytes();
    }

    @Override
    public void map(ImmutableBytesWritable row, Result columns, Context context) {

        KeyValue kv = columns.getColumnLatest(columnFamilyBytes, columnNameBytes);

        if(kv == null || kv.split() == null)
            return;

        Long timestamp = Bytes.toLong(kv.split().getTimestamp());
        Delete delete = new Delete(row.get());
        delete.deleteColumn(columnFamilyBytes, columnNameBytes, timestamp);
        deleteList.add(delete);
    }

    @Override
    protected void cleanup(Context context) {

        String tableName = context.getConfiguration().get(HBaseColumnDropper.TABLE_NAME);
        try {
            HTable table = new HTable(context.getConfiguration(), tableName);
            table.batch(deleteList);
        } catch(Exception e) {
            System.out.println("Error in submitting batch command to hbase");
        }
    }
}

