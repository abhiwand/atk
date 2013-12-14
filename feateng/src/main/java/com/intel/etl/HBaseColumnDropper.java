package com.intel.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;


public class HBaseColumnDropper {

    public static String COLUMN_FAMILY = "column-family";
    public static String COLUMN_NAME = "column-name";
    private static Scan scan = new Scan();

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set(HBaseColumnDropper.COLUMN_FAMILY, "etl-cf");
        conf.set(HBaseColumnDropper.COLUMN_NAME, "src");
        String srcTableName = "test_output_2";

        Job job = new Job(conf, "Drop column");
        job.setJarByClass(HBaseColumnDropper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        TableMapReduceUtil.initTableMapperJob(srcTableName, scan, HBaseColumnDropperMapper.class, IntWritable.class, IntWritable.class, job);
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
    }
}
