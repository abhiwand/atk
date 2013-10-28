

package com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.titanwriter;

import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.TitanConfig;
import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.InputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

/**
 * This chain of two MapReduce jobs loads a property graph into Titan.
 *
 * The first mapper, which generates a multiset of property graph elements,
 * is determined by the input configuration, which is a parameter to the run method.
 *
 * At the first reducer, vertices are gathered by the hash of their ID, and edges
 * are gathered by the hashes of their source vertex IDs, see @code SourceVertexKeyFunction
 *
 * The first reducer performs the following tasks:
 *   - it removes any duplicate edges or vertices  (property maps are combined for duplicates)
 *   - vertices are stored in Titan
 *   - a temporary HDFS file is generated containing property graph elements annotated as follows:
 *   -- vertices are annotated with their Titan IDs
 *   -- edges are annotated with the Titan IDs of their source vertex
 *
 * At the second reducer, vertices are gathered by the hash of their ID, and edges
 * are gathered by the hashes of their destination vertex IDs, see @code DestinationVertexKeyFunction
 *
 * The second reducer loads the edges into Titan.
 *

 * @see InputConfiguration
 * @see GraphTokenizer
 */

public class TitanWriterMRChain extends GraphGenerationMRJob  {

    private static final Logger LOG = Logger.getLogger(TitanWriterMRChain.class);
    private static final int RANDOM_MIN = 1;
    private static final int RANDOM_MAX = 1000000;
    private Configuration    conf;

    private HBaseUtils hbaseUtils = null;
    private boolean    usingHBase = false;

    private GraphTokenizer     tokenizer;
    private InputConfiguration inputConfiguration;

    private PropertyGraphElement mapValueType;
    private Class                vidClass;

    private Functional vertexReducerFunction;
    private Functional edgeReducerFunction;
    private boolean    cleanBidirectionalEdge;



    /**
     * Create the MR Chain object
     *
     */

    @Override
    public void init(InputConfiguration inputConfiguration, GraphTokenizer tokenizer) {

        this.tokenizer          = tokenizer;
        this.inputConfiguration = inputConfiguration;
        this.usingHBase         = inputConfiguration.usesHBase();

        if (usingHBase) {
            try {
                this.hbaseUtils = HBaseUtils.getInstance();
            } catch (IOException e) {
                GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_CONNECT_TO_HBASE,
                        "Cannot allocate the HBaseUtils object. Check hbase connection.", LOG, e);
            }
                this.conf = hbaseUtils.getConfiguration();
        } else {
            this.conf = new Configuration();
        }
    }

    /**
     * Set user defined function for reduce duplicate vertex and edges.
     *
     * @param vertexReducerFunction
     * @param edgeReducerFunction
     */

    public void setFunctionClass(Class vertexReducerFunction, Class edgeReducerFunction) {
        try {
            if (vertexReducerFunction != null) {
                this.vertexReducerFunction = (Functional) vertexReducerFunction.newInstance();
            }

            if (edgeReducerFunction != null) {
                this.edgeReducerFunction = (Functional) edgeReducerFunction.newInstance();
            }
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Unable to instantiate reducer functions.", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when instantiating reducer functions.", LOG, e);
        }
    }

    /**
     * Set the option to clean bidirectional edges.
     *
     * @param clean the boolean option value, if true then clean bidirectional edges.
     */

    @Override
    public void setCleanBidirectionalEdges(boolean clean) {
        cleanBidirectionalEdge = clean;
    }

    /**
     * Set the intermediate key value class.
     *
     * @param valueClass
     */

    @Override
    public void setValueClass(Class valueClass) {
        try {
            this.mapValueType = (PropertyGraphElement) valueClass.newInstance();
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Cannot set value class ( " + valueClass.getName() + ")", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when setting value class ( " + valueClass.getName() + ")", LOG, e);
        }
    }

    /**
     * set the vertex id class
     */

    @Override
    public void setVidClass(Class vidClass) {
        this.vidClass = vidClass;
    }

    /**
     * @return Configuration of the current job
     */

    public Configuration getConf() {
        return this.conf;
    }


    public Integer random(){
        Random rand = new Random();

        int randomNum = rand.nextInt((RANDOM_MAX - RANDOM_MIN) + 1) + RANDOM_MIN;

        return randomNum;

    }
    /**
     * Set the user defined options.
     *
     * @param userOpts a Map of option key value pairs.
     */
    @Override
    public void setUserOptions(HashMap<String, String> userOpts) {
        Set<String> keys = userOpts.keySet();
        for (String key : keys)
            conf.set(key, userOpts.get(key.toString()));
    }

    public void run(CommandLine cmd)
            throws IOException, ClassNotFoundException, InterruptedException {


        String intermediateDataFileName = "graphbuilder_temp_file-" + random().toString();
        Path   intermediateDataFilePath = new Path("/tmp/" + intermediateDataFileName);

        runReadInputLoadVerticesMRJob(intermediateDataFilePath, cmd);

        runEdgeLoadMRJob(intermediateDataFilePath);
    }

    public void runReadInputLoadVerticesMRJob(Path intermediateDataFilePath, CommandLine cmd)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Set required parameters in configuration

        conf.set("GraphTokenizer", tokenizer.getClass().getName());
        conf.setBoolean("noBiDir", cleanBidirectionalEdge);
        conf.set("vidClass", vidClass.getName());
        conf.set("KeyFunction", SourceVertexKeyFunction.class.getName());

        // Set optional parameters in configuration

        if (vertexReducerFunction != null) {
            conf.set("vertexReducerFunction", vertexReducerFunction.getClass().getName());
        }
        if (edgeReducerFunction != null) {
            conf.set("edgeReducerFunction", edgeReducerFunction.getClass().getName());
        }

        // set the configuration per the input

        inputConfiguration.updateConfigurationForMapper(conf, cmd);

        // create loadVerticesJob from configuration and initialize MR parameters

        Job loadVerticesJob = new Job(conf, "TitanWriterMRChain Job 1: Writing Vertices into Titan");
        loadVerticesJob.setJarByClass(TitanWriterMRChain.class);

        // configure mapper  and input

        inputConfiguration.updateJobForMapper(loadVerticesJob, cmd);

        loadVerticesJob.setMapOutputKeyClass(IntWritable.class);
        loadVerticesJob.setMapOutputValueClass(mapValueType.getClass());

        // configure reducer  output

        loadVerticesJob.setReducerClass(VerticesIntoTitanReducer.class);


        // check that the graph database is up and running...
        GraphDatabaseConnector.checkTitanInstallation();

        // now we set up those temporary storage locations...;

        loadVerticesJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        loadVerticesJob.setOutputKeyClass(IntWritable.class);
        loadVerticesJob.setOutputValueClass(mapValueType.getClass());

        FileOutputFormat.setOutputPath(loadVerticesJob, intermediateDataFilePath);

        // fired up and ready to go!

        LOG.info("=========== TitanWriterMRChain Job 1: Create initial  graph elements from raw data, load vertices to Titan ===========");

        LOG.info("input: " + inputConfiguration.getDescription());
        LOG.info("Output is a titan dbase = " +  TitanConfig.config.getProperty("TITAN_STORAGE_TABLENAME"));

        LOG.info("InputFormat = " + inputConfiguration.getDescription());
        LOG.info("GraphTokenizerFromString = " + tokenizer.getClass().getName());

        if (vertexReducerFunction != null) {
            LOG.info("vertexReducerFunction = " + vertexReducerFunction.getClass().getName());
        }

        if (edgeReducerFunction != null) {
            LOG.info("edgeReducerFunction = " + edgeReducerFunction.getClass().getName());
        }

        LOG.info("==================== Start ====================================");
        loadVerticesJob.waitForCompletion(true);
        LOG.info("=================== Done ====================================\n");

    }

    public void runEdgeLoadMRJob(Path intermediateDataFilePath)
            throws IOException, ClassNotFoundException, InterruptedException {

        // create MR Job to load edges into Titan from configuration and initialize MR parameters

        Job writeEdgesJob = new Job(conf, "TitanWriterMRChain Job 2: Writing Edges to Titan");
        writeEdgesJob.setJarByClass(TitanWriterMRChain.class);

        // configure mapper  and input

        writeEdgesJob.setMapperClass(PassThruMapperIntegerKey.class);

        writeEdgesJob.setMapOutputKeyClass(IntWritable.class);
        writeEdgesJob.setMapOutputValueClass(mapValueType.getClass());

        // configure reducer

        writeEdgesJob.setReducerClass(EdgesIntoTitanReducer.class);

        // we read from the temporary storage location...

        writeEdgesJob.setInputFormatClass(SequenceFileInputFormat.class);

        try {
            FileInputFormat.addInputPath(writeEdgesJob, intermediateDataFilePath);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "Cannot access temporary edge file.", LOG, e);
        }

        writeEdgesJob.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);

        LOG.info("=========== Job 2: Propagate Titan Vertex IDs to Edges and Load Edges into Titan  ===========");

        LOG.info("==================== Start ====================================");
        writeEdgesJob.waitForCompletion(true);
        LOG.info("=================== Done ====================================\n");

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(intermediateDataFilePath, true);
    }
}
