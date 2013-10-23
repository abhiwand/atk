
package com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;



import java.io.IOException;


public class WikiPageInputFormat extends TextInputFormat {

    public static final String START_TAG = "<page>";
    public static final String END_TAG   = "</page>";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {

        Configuration conf = context.getConfiguration();
        conf.set(XMLInputFormat.START_TAG_KEY, START_TAG);
        conf.set(XMLInputFormat.END_TAG_KEY,   END_TAG);

        XMLInputFormat.XMLRecordReader xmlRecordReader = null;
        try {
            xmlRecordReader = new XMLInputFormat.XMLRecordReader(context);
            xmlRecordReader.initialize(split, context);
        }
        catch (IOException e) {
            System.exit(1);
        }

        return xmlRecordReader;
    }
}