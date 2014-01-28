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

    List<Delete> deleteList = new ArrayList<Delete>();
    private final int BUCKET_SIZE = 1000;
    HTable table;

    @Override
    protected void setup(Context context) {
        String tableName = context.getConfiguration().get(HBaseColumnDropper.TABLE_NAME);
        try {
            table = new HTable(context.getConfiguration(), tableName);
        } catch(Exception e) {
            throw new RuntimeException("Error in initializing HTable object");
        }
    }

    @Override
    public void map(ImmutableBytesWritable row, Result columns, Context context) {

        Configuration conf = context.getConfiguration();
        String columnNames = conf.get(HBaseColumnDropper.COLUMN_NAME);
        byte[] columnFamilyBytes = conf.get(HBaseColumnDropper.COLUMN_FAMILY).getBytes();

        List<String> listColumns = HBaseColumnDropperMapper.splitFields(columnNames);

        for(String column : listColumns) {
            KeyValue kv = columns.getColumnLatest(columnFamilyBytes, column.getBytes());

            if(kv == null || kv.split() == null)
                return;

            Long timestamp = Bytes.toLong(kv.split().getTimestamp());
            Delete delete = new Delete(row.get());
            delete.deleteColumn(columnFamilyBytes, column.getBytes(), timestamp);
            deleteList.add(delete);

            if(deleteList.size() >= BUCKET_SIZE) {
                SendBatchDeleteCommand();
                deleteList.clear();
            }
        }
    }

    @Override
    protected void cleanup(Context context) {
        SendBatchDeleteCommand();
    }

    private void SendBatchDeleteCommand() {

        if(deleteList.isEmpty())
            return;

        try {
            table.batch(deleteList);
        } catch(Exception e) {
            System.out.println("Error in submitting batch command to hbase");
        }
    }

    public static List<String> splitFields(String colNames) {
        String[] splits = colNames.split(",");
        List<String> fields = new ArrayList<String>();
        for(String field : splits) {
            fields.add(field.trim());
        }
        return fields;
    }
}

