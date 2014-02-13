package com.intel.graph;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class GraphExporter {

    public static final String FILE = "file";
    public static final String METADATA_FILE_PREFIX = "metadata_";
    public static final String SCHEMA = "schema";
    public static final String FEATURE = "feature";
    public static final String JOB_NAME = "Export graph";

    public static void main(String[] args) throws ParseException, ParserConfigurationException, SAXException, IOException, InterruptedException, ClassNotFoundException {

        Parser parser = new PosixParser();
        Options options = new Options();
        options.addOption("f", true, "File Name");
        options.addOption("o", true, "Faunus query output directory");
        options.addOption("q", true, "query definition xml");
        options.addOption("t", true, "graph table name");
        CommandLine cmd = parser.parse(options, args);

        String fileName = cmd.getOptionValue("f");
        String faunusBinDir = new File(System.getenv("FAUNUS_HOME"), "bin").toString();
        String faunusGremlinFile = new File(faunusBinDir, "gremlin.sh").toString();
        String faunusPropertiesFile = new File(faunusBinDir, "titan-hbase-input.properties").toString();

        Path outputDir = new Path(cmd.getOptionValue("o"));
        Path queryOutputDir = new Path(new File(outputDir.toString(), "query").toString());
        Path exporterOutputDir = new Path(new File(outputDir.toString(), "exporter").toString());
        String queryXML = cmd.getOptionValue("q");

        Configuration conf = new Configuration();
        conf.set(FILE, fileName);
        Job job = new Job(conf, JOB_NAME);
        job.setJarByClass(GraphExporter.class);
        job.setMapperClass(GraphExportMapper.class);
        job.setReducerClass(GraphExportReducer.class);

        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        List<String> statements = getStatementListFromXMLString(queryXML);
        String tableName = cmd.getOptionValue("t");
        for (int i = 0; i < statements.size(); i++) {
            String statement = statements.get(i);
            String subStepOutputDir = new File(queryOutputDir.toString(), "step-" + i).toString();
            String[] commandArray = new String[]{faunusGremlinFile, "-i", faunusPropertiesFile, statement, "-Dfaunus.output.location=" + subStepOutputDir + " faunus.graph.input.titan.storage.tablename=" + tableName};
            ProcessBuilder builder = new ProcessBuilder(commandArray);

            builder.redirectErrorStream(true);
            Process p = builder.start();
            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));

            String line = "";
            while ((line = stdInput.readLine ()) != null) {
                System.out.println (line);
            }

            p.waitFor();

            FileStatus[] fileStatuses = fs.listStatus(new Path(subStepOutputDir));

            /* Faunus generates a folder for each map reduce job. For query that requires multiple map reduce
            * jobs, the query result is written to the last created folder. */
            Path resultFolder = getResultFolder(fileStatuses);

            /* The query results are return to sideeffect files */
            Path resultFile = new Path(resultFolder, "sideeffect*");

            /* add the query output to exporter's input path for collecting the query results */
            TextInputFormat.addInputPath(job, resultFile);
        }

        TextOutputFormat.setOutputPath(job, exporterOutputDir);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    public static List<String> getStatementListFromXMLString(String queryXML) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(queryXML));
        Document doc = dBuilder.parse(is);

        List<String> statements = new ArrayList<String>();
        NodeList nList = doc.getElementsByTagName("statement");
        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);
            String statement = nNode.getFirstChild().getNodeValue();
            statements.add(statement);
        }
        return statements;
    }

    public static Path getResultFolder(FileStatus[] fileStatuses) {

        if (fileStatuses == null || fileStatuses.length == 0)
            throw new RuntimeException("FileStatus list is empty.");

        Path result = fileStatuses[0].getPath();
        for (FileStatus status : fileStatuses) {
            int digit = Integer.parseInt(status.getPath().getName().replace("job-", ""));
            int currentMaxDigit = Integer.parseInt(result.getName().replace("job-", ""));

            if (digit > currentMaxDigit)
                result = status.getPath();
        }
        return result;
    }
}
