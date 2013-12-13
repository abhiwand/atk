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
package com.intel.hadoop.graphbuilder.pipeline.output.textgraph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.ElementIdKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
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
 * Sets up a MapReduce job to store a property graph as a text vertex list and a text edge list.
 *
 * <p>
 *     To run a graph construction job:
 *     <ul>
 *         <li>Configure the graph building pipeline by providing a {@code InputConfiguration} and 
 *         {@code GraphBuildingRule} through the method {@code init}.</li>
 *         <li>Invoke the pipeline with the method {@code run}.</li>
 *     </ul>
 * </p>
 * <p>
 *     <ul>
 * <li>The mapper for the job is determined by the {@code InputConfiguration}. </li>
 * <li>The property graph elements are streamed out of the mapper by the {@code GraphTokenizer}.</li>
 * <li>The reducer, provided by this class, performs a "de-duplification" step, by which duplicate 
 * edges and vertices are merged, and then the reducer stores the unique vertices in a text file of 
 * vertices and a text file of edges.</li>
 * </p>
 *
 * <p> The output is structured as follows:
 * <ul>
 * <li>$outputdir/edata Contains the edge data output.</li>
 * <li>$outputdir/vdata Contains the vertex data output.</li>
 * </ul>
 * </p>
 *
 * @see InputConfiguration
 * @see GraphBuildingRule
 * @see TextGraphReducer
 */

public class TextGraphMR extends GraphGenerationMRJob {

    private static final Logger LOG = Logger.getLogger(TextGraphMR.class);

    private Configuration conf;

    private HBaseUtils hbaseUtils = null;
    private boolean    usingHBase = false;
    private String     outputPathName;

    private GraphBuildingRule  graphBuildingRule;
    private InputConfiguration inputConfiguration;

    private SerializedGraphElement mapValueType;
    private Class                vidClass;

    private final Class          keyFuncClass = ElementIdKeyFunction.class;

    private Functional vertexReducerFunction;
    private Functional edgeReducerFunction;
    private boolean    cleanBidirectionalEdge;

    /**
     * The constructor. It requires the pathname for the output as an argument.
     * @param outputPathName  the pathname as a String
     */
    public TextGraphMR(String outputPathName) {
        this.outputPathName = outputPathName;
    }

    /**
     * Set-up time routine that connects raw data ({@code inputConfiguration} and the graph 
     * generations rule ({@code graphBuildingRule}) to the MR chain.
     *
     * <p>
     *     This step must be taken before attempting to execute the pipeline with the {@code run} method.
     * </p>
     *
     * @param inputConfiguration The object that handles the generation of data records from raw data.
     * @param graphBuildingRule The object that handles the conversion of data records into a 
	 * property graph element.
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
            this.conf       = hbaseUtils.getConfiguration();
        } else {
            this.conf = new Configuration();
        }
    }

    /**
     * Sets user defined functions to reduce duplicate vertices and edges.
     * <p>If the user does not specify these functions, the default behavior is that duplicate objects 
     * will be merged by having their property maps merged.</p>
     *
     * @param vertexReducerFunction User specified function for reducing duplicate vertices.
     * @param edgeReducerFunction   User specified function for reducing duplicate edges.
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
     * @param clean The boolean option value, if true then remove bidirectional edges.
     */

    @Override
    public void setCleanBidirectionalEdges(boolean clean) {
        cleanBidirectionalEdge = clean;
    }

    /**
     *Sets the value class for the property graph elements coming from the mapper and tokenizer.
     *
     * <p> The class is one of the instantiations of {@code SerializedGraphElement}that determines
     * the vertex ID type.</p>
     *
     * @param valueClass The intermediate value class
     * @see com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement
     * @see com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementLongTypeVids
     * @see com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids
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
     * Set the vertex id class.
     * <p>This can either be a {@code StringType} or {@code LongType}, which are writable 
     * encapsulations of the {@code String} and {@code Long} types, respectively. </p>
     * @param vidClass The class of the vertex IDs.
     * @see com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement
     * @see com.intel.hadoop.graphbuilder.types.StringType
     * @see com.intel.hadoop.graphbuilder.types.LongType
     */

    @Override
    public void setVidClass(Class vidClass) {
        this.vidClass = vidClass;
    }

    /**
     * Get the configuration of the current job.
     * @return The Hadoop configuration of the current job.
     */

    public Configuration getConf() {
        return this.conf;
    }

    /**
     * Sets user defined options.
     *
     * @param userOpts A Map of option key value pairs.
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
     * @param cmd The user provided command line options.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */

    // dev todo:  the cmd parameter is deprecated and not used any more by this method ---
    // it has not yet been eliminated because it has not been eliminated from the GraphGenerationMRJob abstract class
    // which is blocked by getting the command lines out of the other reducers
    @Override
    public void run(CommandLine cmd) throws IOException, ClassNotFoundException, InterruptedException {

        // Set required parameters in configuration

        conf.set("GraphTokenizer", graphBuildingRule.getGraphTokenizerClass().getName());
        conf.setBoolean("noBiDir", cleanBidirectionalEdge);
        conf.set("vidClass", vidClass.getName());
        conf.set("KeyFunction", keyFuncClass.getName());

        // Set optional parameters in configuration

        if (vertexReducerFunction != null) {
            conf.set("vertexReducerFunction", vertexReducerFunction.getClass().getName());
        }
        if (edgeReducerFunction != null) {
            conf.set("edgeReducerFunction", edgeReducerFunction.getClass().getName());
        }

        // set the configuration per the input

        inputConfiguration.updateConfigurationForMapper(conf);

        // update the configuration per the graphBuildingRule

        graphBuildingRule.updateConfigurationForTokenizer(conf);

        // create job from configuration and initialize MR parameters

        Job job = new Job(conf, "TextGraphMR");
        job.setJarByClass(TextGraphMR.class);

        // configure mapper  and input

        inputConfiguration.updateJobForMapper(job);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(mapValueType.getClass());

        // configure reducer

        job.setReducerClass(TextGraphReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // configure output path
        // (input path handled by the inputconfiguration)

        job.setOutputFormatClass(TextOutputFormat.class);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPathName));

        // fired up and ready to go!

        LOG.info("=========== Job: Creating vertex list and edge list from input data, saving as text file ===========");

        LOG.info("input: " + inputConfiguration.getDescription());
        LOG.info("Output = " + outputPathName);

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
