

package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.VertexSchema;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.util.*;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.tinkerpop.blueprints.Vertex;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.BaseConfiguration;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

/**
 * Class that handles loading the constructed property graph into Titan.
 *
 * <p>It is a chain of two map reduce jobs.</p>
 *
 * <p>
 * The first mapper, which generates a multiset of property graph elements,
 * is determined by the input configuration and the graph building rule. These two
 * objects are parameters to the {@code init} method.
 * </p>
 *
 * <p>
 * At the first reducer, vertices are gathered by the hash of their ID, and edges
 * are gathered by the hashes of their source vertex IDs, see {@code SourceVertexKeyFunction}
 * </p>
 *
 * The first reducer performs the following tasks:
 * <ul>
 *   <li> Remove any duplicate edges or vertices  (default: property maps are combined for duplicates)</li>
 *   <li> Store vertices in Titan </li>
 *   <li> Create a temporary HDFS file containing property graph elements annotated as follows:
 *   <ul>
 *       <li>vertices are annotated with their Titan IDs  </li>
 *       <li>edges are annotated with the Titan IDs of their source vertex</li>
 *       </ul>
 *       </ul>
 *
 * <p>
 * At the second reducer, vertices are gathered by the hash of their ID, and edges
 * are gathered by the hashes of their destination vertex IDs, see {@code DestinationVertexKeyFunction}
 *  </p>
 *  <p>
 * The second reducer then loads the edges into Titan.
 * </p>
 *
 * @see InputConfiguration
 * @see GraphBuildingRule
 * @see SourceVertexKeyFunction
 * @see com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.DestinationVertexKeyFunction
 */

public class TitanWriterMRChain extends GraphGenerationMRJob  {

    private static final Logger LOG = Logger.getLogger(TitanWriterMRChain.class);
    private static final int RANDOM_MIN = 1;
    private static final int RANDOM_MAX = 1000000;
    private Configuration    conf;

    private HBaseUtils hbaseUtils = null;
    private boolean    usingHBase = false;

    private GraphBuildingRule  graphBuildingRule;
    private InputConfiguration inputConfiguration;

    private PropertyGraphElement mapValueType;
    private Class                vidClass;
    private PropertyGraphSchema  graphSchema;

    private Functional vertexReducerFunction;
    private Functional edgeReducerFunction;
    private boolean    cleanBidirectionalEdge;



    /**
     * Acquire set-up time components necessary for creating gaph from raw data and loading into Titan.
     * @param  inputConfiguration object that handles creation of data records from raw data
     * @param  graphBuildingRule object that handles creation of property graph elements from data records
     */

    @Override
    public void init(InputConfiguration inputConfiguration, GraphBuildingRule graphBuildingRule) {

        this.graphBuildingRule  = graphBuildingRule;
        this.inputConfiguration = inputConfiguration;
        this.graphSchema        = graphBuildingRule.getGraphSchema();
        this.usingHBase         = true;

        try {
            this.hbaseUtils = HBaseUtils.getInstance();
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_CONNECT_TO_HBASE,
                    "Cannot allocate the HBaseUtils object. Check hbase connection.", LOG, e);
        }

        this.conf = hbaseUtils.getConfiguration();

    }

    /**
     * Set user defined function for reduce duplicate vertex and edges.
     *
     * If the use does not specify these function, the default method of merging property lists will be used.s
     *
     * @param vertexReducerFunction   user specified function for reducing duplicate vertices
     * @param edgeReducerFunction     user specified function for reducing duplicate edges
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
     * @param clean the boolean option value, if true then remove bidirectional edges.
     */

    @Override
    public void setCleanBidirectionalEdges(boolean clean) {
        cleanBidirectionalEdge = clean;
    }

    /**
     * Set the value class for the property graph elements coming from the mapper/tokenizer
     *
     * This type can vary depending on the class used for vertex IDs.
     *
     * @param valueClass   class of the PropertyGraphElement value
     * @see PropertyGraphElement
     * @see com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementLongTypeVids
     * @see com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids
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
     * Set the vertex id class
     *
     * Currently long and String are supported.
     * @see PropertyGraphElement
     * @see com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementLongTypeVids
     * @see com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids
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


    private Integer random(){
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


    /**
     * create the titan graph for saving edges and remove the static open method from setup so it can be mocked
     *
     * @return TitanGraph for saving edges
     * @throws IOException
     */
    private TitanGraph tribecaGraphFactoryOpen(Configuration configuration) throws IOException {
        BaseConfiguration titanConfig = new BaseConfiguration();

        return GraphDatabaseConnector.open("titan", titanConfig, configuration);
    }

    /*
     * Open the Titan graph database, and make the Titan keys required by the graph schema.
     */
    private void initTitanGraph () {
        TitanGraph graph = null;

        try {
            graph = tribecaGraphFactoryOpen(conf);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "GRAPHBUILDER FAILURE: Unhandled IO exception while attempting to connect to Titan.",  LOG, e);
        }

        graph.makeKey("trueName").dataType(String.class).indexed(Vertex.class).unique().make();

        for (VertexSchema vertexSchema : graphSchema.getVertexSchemata())  {
            for (PropertySchema vps : vertexSchema.getPropertySchemata()) {
                graph.makeKey(vps.getName()).dataType(vps.getType()).make();
            }
        }


        for (EdgeSchema edgeSchema : graphSchema.getEdgeSchemata())  {
            ArrayList<TitanKey> titanKeys = new ArrayList<TitanKey>();

            for (PropertySchema eps : edgeSchema.getPropertySchemata()) {
                titanKeys.add(graph.makeKey(eps.getName()).dataType(eps.getType()).make());
            }

            TitanKey[] titanKeyArray = titanKeys.toArray(new TitanKey[titanKeys.size()]);
            graph.makeLabel(edgeSchema.getLabel()).signature(titanKeyArray).make();
        }

        graph.commit();
    }

    /**
     * Execute the MR chain that constructs a graph from raw input specified
     * by {@code InputConfiguration} according to the graph construction rule {@code GraphBuildingRule}
     * and load it into the Titan graph database
     *
     * @param cmd  User specified command line
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void run(CommandLine cmd)
            throws IOException, ClassNotFoundException, InterruptedException {

        // warn the user if the Titan table already exists in Hbase

        String titanTableName = TitanConfig.config.getProperty("TITAN_STORAGE_TABLENAME");

        if (hbaseUtils.tableExists(titanTableName)) {
            if (cmd.hasOption(CommonCommandLineOptions.Option.titanAppend.get())) {
            LOG.info("WARNING:  hbase table " + titanTableName +
                     " already exists. Titan will append new graph to existing data.");
            } else {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                        "GRAPHBUILDER FAILURE: hbase table " + titanTableName +
                                " already exists. Use -a option if you wish to append new graph to existing data.", LOG);
            }
        }

        String intermediateDataFileName = "graphbuilder_temp_file-" + random().toString();
        Path   intermediateDataFilePath = new Path("/tmp/" + intermediateDataFileName);


        initTitanGraph();

        runReadInputLoadVerticesMRJob(intermediateDataFilePath, cmd);

        runEdgeLoadMRJob(intermediateDataFilePath);

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(intermediateDataFilePath, true);
    }

    private void runReadInputLoadVerticesMRJob(Path intermediateDataFilePath, CommandLine cmd)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Set required parameters in configuration

        conf.set("GraphTokenizer", graphBuildingRule.getGraphTokenizerClass().getName());
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

        // update the configuration per the tokenizer

        graphBuildingRule.updateConfigurationForTokenizer(conf, cmd);

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
        LOG.info("GraphBuildingRule = " + graphBuildingRule.getClass().getName());

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

    private void runEdgeLoadMRJob(Path intermediateDataFilePath)
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
    }
}
