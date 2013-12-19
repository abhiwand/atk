/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.output.rdfgraph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.input.rdf.RDFConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.ElementIdKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.util.Functional;
/**
 * Sets up a Map Reduce job to store graph elements as RDF triples.
 *
 * <p>
 *     To run a RDF graph construction job:
 *     <ul>
 *         <li>Configure the graph building pipeline by providing an {@code InputConfiguration} and 
 *         {@code GraphBuildingRule} through the {@code init} method.</li>
 *         <li>Invoke the pipeline with the {@code run} method.</li>
 *     </ul>
 * </p>
 * <p>
 *     <ul>
 *         <li>The mapper for the job is determined by the {@code InputConfiguration}. </li>
 *         <li>The property graph elements are streamed out of the mapper by 
 *             the {@code GraphTokenizer}.</li>
 *         <li>The reducer, provided by this class, performs a "de-duplification" step, 
 *             in which duplicate edges and vertices are merged, and then stores the unique 
 *             vertices in a text file of vertices and a text file of edges.</li>
 *     </ul>
 * </p>
 *
 * <p> The output is structured as follows:
 * <ul>
 * <li>{@code $outputdir/edata} Contains the edge data output.</li>
 * <li>{@code $outputdir/vdata} Contains the vertex data output.</li>
 * </ul>
 * </p>
 *
 * @see InputConfiguration
 * @see GraphBuildingRule
 * @see RDFGraphReducer
 */

public class RDFGraphMR extends GraphGenerationMRJob {

    private static final Logger LOG = Logger.getLogger(RDFGraphMR.class);

    private Configuration conf;

    private HBaseUtils hbaseUtils = null;
    private boolean    usingHBase = false;

    private GraphBuildingRule  graphBuildingRule;
    private InputConfiguration inputConfiguration;

    private SerializedGraphElement mapValueType;
    private Class vidClass;

    private final Class keyFuncClass = ElementIdKeyFunction.class;

    private Functional vertexReducerFunction;
    private Functional edgeReducerFunction;
    private boolean    cleanBidirectionalEdge;
    private String     outputPathName;

    /**
     * The constructor. It requires the pathname for the output as an argument.
     * @param {@code outputPathName}  The pathname as a String.
     */
    public RDFGraphMR(String outputPathName) {
        this.outputPathName = outputPathName;
    }

    /**
     * A set-up time routine that connects raw data ({@code inputConfiguration} and the graph generations rule
     * ({@code graphBuildingRule}) into the Map Reduce chain.
     *
     * <p>
     *     This step must be taken before attempting to execute the pipeline with the {@code run} method.
     * </p>
     *
     * @param {@code inputConfiguration} The object that handles the generation of data records from raw data.
     * @param {@code graphBuildingRule}  The object that handles the conversion of data records into property graph element.
     */
    @Override
    public void init(InputConfiguration inputConfiguration, GraphBuildingRule graphBuildingRule) {

        this.graphBuildingRule  = graphBuildingRule;
        this.inputConfiguration = inputConfiguration;
        this.usingHBase         = inputConfiguration.usesHBase();

        if (usingHBase) {
            try {
                this.hbaseUtils = HBaseUtils.getInstance();
            } catch (IOException e) {
                GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_CONNECT_TO_HBASE,
                        "GRAPHBUILDER_ERROR: Cannot allocate the HBaseUtils object. Check hbase connection.", LOG, e);
            }
            this.conf = hbaseUtils.getConfiguration();
        } else {
            this.conf = new Configuration();
        }
    }

    /**
     * Sets the user defined function for reducing duplicate vertices and edges.
     * <p>If the user does not specify these functions, the default behavior is that the duplicate 
     * objects will be merged by having their property maps merged.</p>
     *
     * @param {@code vertexReducerFunction} The user specified function for reducing duplicate vertices.
     * @param {@code edgeReducerFunction}   The user specified function for reducing duplicate edges.
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
                    "GRAPHBUILDER_ERROR: Cannot instantiate reducer functions.", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Illegal access exception when instantiating reducer functions.", LOG, e);
        }
    }

    /**
     * Sets the option to clean (remove) bidirectional edges.
     *
     * @param {@code clean}  The boolean option value, if true then remove bidirectional edges.
     */

    @Override
    public void setCleanBidirectionalEdges(boolean clean) {
        cleanBidirectionalEdge = clean;
    }

    /**
     * Sets the value class for the property graph elements coming from the mapper or tokenizer.
     *
     * <p> This class is one of the instantiations of {@code GraphElement}, determined by the vertex ID type.</p>
     *
     * @param {@code valueClass} The intermediate value class.
     * @see com.intel.hadoop.graphbuilder.graphelements.GraphElement
     * @see com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids
     * @see com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementLongTypeVids
     */

    @Override
    public void setValueClass(Class valueClass) {
        try {
            this.mapValueType = (SerializedGraphElement) valueClass.newInstance();
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Cannot set value class ( " + valueClass.getName() + ")", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Illegal access exception when setting value class ( " + valueClass.getName() + ")", LOG, e);
        }
    }

    /**
     * Sets the vertex id class.
     * <p>This can be either a {@code StringType} or {@code LongType}, which are writable encapsulations of the
     * {@code String} and {@code Long} types, respectively. </p>
     * @param {@code vidClass} The class of the vertex IDs.
     * @see com.intel.hadoop.graphbuilder.graphelements.GraphElement
     * @see com.intel.hadoop.graphbuilder.types.StringType
     * @see com.intel.hadoop.graphbuilder.types.LongType
     */

    @Override
    public void setVidClass(Class vidClass) {
        this.vidClass = vidClass;
    }

    /**
     * Gets the configuration of the current job.
     * @return The Hadoop configuration of the current job.
     */

    public Configuration getConf() {
        return this.conf;
    }

    /**
     * Sets user defined options.
     *
     * @param {@code userOpts} A Map of the option key value pairs.
     */
    @Override
    public void setUserOptions(HashMap<String, String> userOpts) {
        Set<String> keys = userOpts.keySet();
        for (String key : keys)
            conf.set(key, userOpts.get(key.toString()));
    }

    /**
     * Runs the graph building pipeline.
     *
     * @param {@code cmd} The user provided command line options.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */

    public void run(CommandLine cmd) throws IOException, ClassNotFoundException, InterruptedException {

        // Sets the required parameters in the configuration.

        conf.set("GraphTokenizer", graphBuildingRule.getGraphTokenizerClass().getName());
        conf.setBoolean("noBiDir", cleanBidirectionalEdge);
        conf.set("vidClass", vidClass.getName());
        conf.set("KeyFunction", keyFuncClass.getName());

        // Sets the optional parameters in the configuration.

        if (vertexReducerFunction != null) {
            conf.set("vertexReducerFunction", vertexReducerFunction.getClass().getName());
        }
        if (edgeReducerFunction != null) {
            conf.set("edgeReducerFunction", edgeReducerFunction.getClass().getName());
        }

        // Sets the configuration per the input, for example, set the input
        // HBase table name if the input type is HBase.

        inputConfiguration.updateConfigurationForMapper(conf);

        // Updates the configuration per the graphBuildingRule.

        graphBuildingRule.updateConfigurationForTokenizer(conf);

        // Adds the RDF namespace.
        conf.set(RDFConfiguration.config.getProperty("CMD_RDF_NAMESPACE"), cmd.getOptionValue("n"));

        // Creates a job from the configuration and initialization Map Reduce parameters.

        Job job = new Job(conf, "RDFGraphMR");
        job.setJarByClass(RDFGraphMR.class);

        // Configures the mapper and input.

        inputConfiguration.updateJobForMapper(job);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(mapValueType.getClass());

        // Configures the reducer.

        job.setReducerClass(RDFGraphReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Configures the output path.
        // (The input path is handled by the inputconfiguration.)

        job.setOutputFormatClass(TextOutputFormat.class);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(this.outputPathName));

        // Fired up and ready to go!

        LOG.info("=========== Job: Creating vertex list and edge list from input data, saving as text file ===========");

        LOG.info("Input: " + inputConfiguration.getDescription());
        LOG.info("Output = " + this.outputPathName);

        LOG.info("InputFormat = " + inputConfiguration.getDescription());
        LOG.info("GraphTokenizerFromString = " + graphBuildingRule.getClass().getName());

        if (vertexReducerFunction != null) {
            LOG.info("vertexReducerFunction = " + vertexReducerFunction.getClass().getName());
        }

        if (edgeReducerFunction != null) {
            LOG.info("edgeReducerFunction = " + edgeReducerFunction.getClass().getName());
        }

        LOG.info("==================== Start ====================================");
        job.waitForCompletion(true);
        LOG.info("=================== Done ====================================\n");
    }
}
