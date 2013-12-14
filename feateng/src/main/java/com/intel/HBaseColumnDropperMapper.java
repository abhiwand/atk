package com.intel;

import com.intel.etl.HBaseColumnDropper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 12/13/13
 * Time: 2:23 PM
 * To change this template use File | Settings | File Templates.
 */


public class HBaseColumnDropperMapper extends TableMapper<IntWritable, IntWritable> {

    HTable table;
    @Override
    public void setup(Context context) {
        //table = new HTable();
    }

    @Override
    public void map(ImmutableBytesWritable row, Result columns, Context context) {

        Configuration conf = context.getConfiguration();
        String columnFamily = conf.get(HBaseColumnDropper.COLUMN_FAMILY);
        String columnName = conf.get(HBaseColumnDropper.COLUMN_NAME);

        if("65123".equalsIgnoreCase(Bytes.toString(row.get())))  {
            Delete delete = new Delete(row.get());
            System.out.println(columnFamily);
            System.out.println(columnName);
            System.out.println("start...");
            System.out.println("timestamp:" + HConstants.LATEST_TIMESTAMP);

            delete.deleteColumn(columnFamily.getBytes(), columnName.getBytes());

            System.out.println("end...");
        }
    }
}

