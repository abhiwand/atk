package com.intel.etl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;


public class HBaseColumnDropper {

    public static final String COLUMN_FAMILY = "column-family";
    public static final String COLUMN_NAME = "column-name";
    public static final String TABLE_NAME = "table-name";
    private static Scan scan = new Scan();

    public static void main(String[] args) throws Exception {

        Parser parser = new PosixParser();
        Options options = new Options();
        options.addOption("f", true,  "Column Family");
        options.addOption("n", true,  "Column Name");
        options.addOption("t", true,  "Table Name");
        CommandLine cmd = parser.parse(options, args);


        String srcTableName = cmd.getOptionValue("t");
        String columnFamily = cmd.getOptionValue("f");
        String columnName = cmd.getOptionValue("n");

        Configuration conf = new Configuration();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        conf.set(HBaseColumnDropper.TABLE_NAME, srcTableName);
        conf.set(HBaseColumnDropper.COLUMN_FAMILY, columnFamily);
        conf.set(HBaseColumnDropper.COLUMN_NAME, columnName);

        Job job = new Job(conf, "Drop column");
        job.setJarByClass(HBaseColumnDropper.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);

        TableMapReduceUtil.initTableMapperJob(srcTableName, scan, HBaseColumnDropperMapper.class, IntWritable.class, IntWritable.class, job);
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
    }
}
