package com.intel.graph;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * GraphExporter runs faunus queries, collects query results and
 * then generates a graphml file based on the query results.
 */
public class GraphExporter {

    public static final String FILE = "file";
    public static final String METADATA_FILE_PREFIX = "metadata_";
    public static final String SCHEMA = "schema";
    public static final String FEATURE = "feature";
    public static final String JOB_NAME = "Export graph";

    /**
     * Run faunus query then run map-reduce job to collect the result and generate a graphml file.
     * @param args
     * @throws ParseException
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws XMLStreamException
     */
    public static void main(String[] args) throws ParseException, ParserConfigurationException, SAXException, IOException, InterruptedException, ClassNotFoundException, XMLStreamException {

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
        final Job job = new Job(conf, JOB_NAME);
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
            executeFaunusQuery(faunusGremlinFile, faunusPropertiesFile, tableName, statement, subStepOutputDir);


            final IPathCollector collector = new IPathCollector() {
                @Override
                public void collectPath(Path path) {
                    try {
                        TextInputFormat.addInputPath(job, path);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to add query output for processing");
                    }
                }
            };
            addQueryOutputToInputPath(fs, subStepOutputDir, collector);
        }

        Path[] eligibleInputPaths = TextInputFormat.getInputPaths(job);
        if(eligibleInputPaths == null || eligibleInputPaths.length == 0) {
            writeEmptyGraphML(fileName);
            return;
        }

        TextOutputFormat.setOutputPath(job, exporterOutputDir);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    /**
     * Execute faunus query
     * @param faunusGremlinFile: The path to gremlin.sh under Faunus installation
     * @param faunusPropertiesFile: The path to input properties file for executing Faunus query
     * @param tableName: The graph table name
     * @param statement: Faunus query statement
     * @param subStepOutputDir: The output directory for this query
     * @throws IOException
     * @throws InterruptedException
     */
    static void executeFaunusQuery(String faunusGremlinFile, String faunusPropertiesFile, String tableName, String statement, String subStepOutputDir) throws IOException, InterruptedException {
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
    }

    /**
     * Add Faunus query output folder to the input path of GraphExporter map-reduce job.
     * @param fs: Filesystem instance
     * @param subStepOutputDir: The output directory for this query
     * @param collector: path collector instance to collect Faunus query output path
     * @throws IOException
     */
    static void addQueryOutputToInputPath(FileSystem fs, String subStepOutputDir, IPathCollector collector) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(subStepOutputDir));

            /* Faunus generates a folder for each map reduce job. For query that requires multiple map reduce
            * jobs, the query result is written to the last created folder. */
        Path resultFolder = getResultFolder(fileStatuses);

            /* The query results are return to sideeffect files */
        Path resultFile = new Path(resultFolder, "sideeffect*");

        FileStatus[] matches = fs.globStatus(resultFile);
            /* add the query output to exporter's input path for collecting the query results */
        if(matches != null && matches.length > 0)
            collector.collectPath(resultFile);
    }

    /**
     * Generate an graphml which only has starting and end tags. It does not
     * have element data regarding vertices and edges
     * @param fileName: The output file name
     * @throws IOException
     * @throws XMLStreamException
     */
    private static void writeEmptyGraphML(String fileName) throws IOException, XMLStreamException {
        final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        outputFactory.setProperty("escapeCharacters", false);
        FSDataOutputStream outputStream = GraphMLWriter.createFile(fileName, FileSystem.get(new Configuration()));
        XMLStreamWriter writer = outputFactory.createXMLStreamWriter(outputStream, "UTF8");
        GraphMLWriter.writeGraphMLHeaderSection(writer, new HashMap<String, String>(), new HashMap<String, String>());
        GraphMLWriter.writeGraphMLEndSection(writer);
    }

    /**
     * Parse the query statement xml and return a list of strings
     * which contains query statements.
     * @param queryXML
     * @return List of string which contains query statements
     * @throws ParserConfigurationException
     * @throws IOException
     * @throws SAXException
     */
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

    /**
     * Determine which is the result folder. Faunus query can involve multiple
     * step. Each step creates a folder. The query result is written to the last
     * created folder.
     * @param fileStatuses
     * @return Path to the folder which contains the query result
     */
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
