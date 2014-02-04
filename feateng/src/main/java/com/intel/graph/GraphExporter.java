package com.intel.graph;

import java.io.File;
import java.io.IOException;

import com.intel.etl.HBaseColumnDropperMapper;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class GraphExporter {
    public static void main(String[] args) throws ParseException {

        Parser parser = new PosixParser();
        Options options = new Options();
        options.addOption("s", true,  "Schema");
        options.addOption("f", true,  "File Name");
        CommandLine cmd = parser.parse(options, args);
        Path outputDir = new Path("xml_out");
        String fileName = cmd.getOptionValue("f");
        String faunusBinDir = new File(System.getenv("FAUNUS_HOME"), "bin").toString();
        String faunusGremlinFile = new File(faunusBinDir, "gremlin.sh").toString();
        String faunusPropertiesFile = new File(faunusBinDir, "titan-hbase-input.properties").toString();
        //String[] commandArray = new String[]{faunusGremlinFile, "-i", faunusPropertiesFile, "g.V('_gb_ID','11').out.keep", "-Dfaunus.output.location=query_1"};
        String queryOutput = "query_1";
        try {

            /*Runtime r = Runtime.getRuntime();
            Process p = r.exec(commandArray);
            System.out.println("command issued");

            p.waitFor();
            System.out.println("command done");*/
            FileSystem fs = FileSystem.get(new Configuration());

            FileStatus[] fileStatuses = fs.listStatus(new Path(queryOutput));
            Path resultFolder = getResultFolder(fileStatuses);
            Path resultFile = new Path(resultFolder, "sideeffect*");


            Configuration conf = new Configuration();
            conf.set("file", fileName);
            Job job = new Job(conf, "Export graph");
            job.setJarByClass(GraphExporter.class);
            job.setMapperClass(GraphExportMapper.class);
            job.setReducerClass(GraphExportReducer.class);
            TextInputFormat.addInputPath(job, resultFile);


            TextOutputFormat.setOutputPath(job, outputDir);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setNumReduceTasks(1);
            job.waitForCompletion(true);

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public static Path getResultFolder(FileStatus[] fileStatuses) {

        if(fileStatuses == null || fileStatuses.length == 0)
            throw new RuntimeException("FileStatus list is empty.");

        Path result = fileStatuses[0].getPath();
        for(FileStatus status : fileStatuses) {
            int digit = Integer.parseInt(status.getPath().getName().replace("job-", ""));
            int currentMaxDigit = Integer.parseInt(result.getName().replace("job-", ""));

            if(digit > currentMaxDigit)
                result = status.getPath();
        }
        return result;
    }
}
