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

    public static final String FILE = "file";
    public static final String VERTEX_SCHEMA = "Vertex Schema";
    public static final String EDGE_SCHEMA = "Edge Schema";

    public static void main(String[] args) throws ParseException {

        Parser parser = new PosixParser();
        Options options = new Options();
        options.addOption("v", true,  "vertex schema");
        options.addOption("e", true,  "edge schema");
        options.addOption("f", true,  "File Name");
        options.addOption("o", true, "Faunus query output directory");
        options.addOption("q", true, "Faunus query");
        CommandLine cmd = parser.parse(options, args);
        Path outputDir = new Path("xml_out");
        String fileName = cmd.getOptionValue("f");
        String faunusBinDir = new File(System.getenv("FAUNUS_HOME"), "bin").toString();
        String faunusGremlinFile = new File(faunusBinDir, "gremlin.sh").toString();
        String faunusPropertiesFile = new File(faunusBinDir, "titan-hbase-input.properties").toString();
        String queryOutput = cmd.getOptionValue("o");
        String queryString = cmd.getOptionValue("q");

        try {
            FileSystem fs = FileSystem.get(new Configuration());
            String[] statements = queryString.split(";");

            Configuration conf = new Configuration();
            conf.set(FILE, fileName);
            String vertexSchema = cmd.getOptionValue("v");
            conf.set(VERTEX_SCHEMA, vertexSchema);
            String edgeSchema = cmd.getOptionValue("e");
            conf.set(EDGE_SCHEMA, edgeSchema);
            Job job = new Job(conf, "Export graph");
            job.setJarByClass(GraphExporter.class);
            job.setMapperClass(GraphExportMapper.class);
            job.setReducerClass(GraphExportReducer.class);


            for(int i = 0; i < statements.length; i++) {
                String statement = statements[i];
                Runtime r = Runtime.getRuntime();

                String subStepOutputDir = new File(queryOutput, "step-" + i).toString();
                String[] commandArray = new String[]{faunusGremlinFile, "-i", faunusPropertiesFile, statement, "-Dfaunus.output.location=" + subStepOutputDir};
                Process p = r.exec(commandArray);
                System.out.println("command issued");

                p.waitFor();
                System.out.println("command done");

                FileStatus[] fileStatuses = fs.listStatus(new Path(subStepOutputDir));
                Path resultFolder = getResultFolder(fileStatuses);
                Path resultFile = new Path(resultFolder, "sideeffect*");
                TextInputFormat.addInputPath(job, resultFile);
            }

            if (fs.exists(outputDir)) {
                fs.delete(outputDir, true);
            }

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
