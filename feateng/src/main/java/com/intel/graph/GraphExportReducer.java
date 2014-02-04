package com.intel.graph;

import com.intel.etl.HBaseColumnDropper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GraphExportReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    private FSDataOutputStream output;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String fileName = conf.get("file");

        Path path = new Path(fileName);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        output = fs.create(path, true);

        Map<String, String> vertexKeyTypes = new HashMap<String, String>();
        Map<String, String> edgeKeyTypes = new HashMap<String, String>();

    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) {

        for(Text value: values){
            try {
                output.writeBytes(value.toString());
                output.writeBytes("\n");
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

    }
}
