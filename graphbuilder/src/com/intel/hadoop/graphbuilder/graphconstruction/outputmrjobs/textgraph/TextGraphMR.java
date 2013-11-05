/* Copyright (C) 2012 Intel Corporation.
 *     All rights reserved.
 *           
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder 
 */
package com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.textgraph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import com.intel.hadoop.graphbuilder.graphconstruction.keyfunction.ElementIdKeyFunction;
import com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.InputConfiguration;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
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
 * Set up a MapReduce jobs to store a property graph as a text vertex list and a text edge list.
 *
 * <p>
 *     To run a graph construction job:
 *     <ul>
 *         <li>Configure the graph building pipeline by providing a {@code InputConfiguration} and {@code GraphTokenizer}
 *         through the method {@code init}</li>
 *         <li>Invoke the pipeline with the method {@code run}</li>
 *     </ul>
 * </p>
 * <p>
 *     <ul>
 * <li>The mapper for the job is determined by the {@code InputConfiguration}. </li>
 * <li>The property graph elements are streamed out of the mapper by the {@code GraphTokenizer}</li>
 * <li>The reducer, provided by this class, performs a "de-duplification" step, by which duplicate edges and vertices
 * are merged, and then stores the unique vertices in a text file of vertices and a text file of edges.</li>
 * </p>
 *
 * <p> The output is structured as follows:
 * <ul>
 * <li>$outputdir/edata contains edge data output</li>
 * <li>$outputdir/vdata contains vertex data output</li>
 * </ul>
 * </p>
 *
 * @see InputConfiguration
 * @see GraphTokenizer
 */

public class TextGraphMR extends GraphGenerationMRJob {

    private static final Logger LOG = Logger.getLogger(TextGraphMR.class);

    private Configuration conf;

    private HBaseUtils hbaseUtils = null;
    private boolean    usingHBase = false;

    private GraphTokenizer     tokenizer;
    private InputConfiguration inputConfiguration;

    private PropertyGraphElement mapValueType;
    private Class                vidClass;

    private final Class          keyFuncClass = ElementIdKeyFunction.class;

    private Functional vertexReducerFunction;
    private Functional edgeReducerFunction;
    private boolean    cleanBidirectionalEdge;


    /**
     * Configure the graph building pipeline.
     *
     * <p>
     *     This step must be taken before attempting to execute the pipeline with the {@code run} method.
     * </p>
     *
     * @param tokenizer  the graph tokenization rule that streams data records into property graph elements
     * @param inputConfiguration handles reading raw data and converting it into "records" that can be
     *                           processed by the tokenizer
     */
    @Override
    public void init(InputConfiguration inputConfiguration, GraphTokenizer tokenizer) {

        this.tokenizer          = tokenizer;
        this.inputConfiguration = inputConfiguration;
        this.usingHBase         = inputConfiguration.usesHBase();

        if (usingHBase) {
            this.hbaseUtils = HBaseUtils.getInstance();
            this.conf       = hbaseUtils.getConfiguration();
        } else {
            this.conf = new Configuration();
        }
    }

    /**
     * Set user defined function for reduce duplicate vertices and edges.
     * <p>If the user does not specify these functions, the default behavior is that duplicate objects will be
     * merged by having their property maps merged.</p>
     *
     * @param vertexReducerFunction  a method for reducing duplicate vertices
     * @param edgeReducerFunction   a method for reducing duplicate edges
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
            LOG.fatal("Cannot instantiate reducer functions.");
            System.exit(1);
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            LOG.fatal("Error instantiating reducer functions.");
            System.exit(1);
            e.printStackTrace();
        }
    }

    /**
     * Set the option to clean (remove) bidirectional edges.
     *
     * @param clean the boolean option value, if true then clean bidirectional edges.
     */

    @Override
    public void setCleanBidirectionalEdges(boolean clean) {
        cleanBidirectionalEdge = clean;
    }

    /**
     * Set the intermediate value class.
     *
     * <p>Always one of the instantiations of a property graph element</p>
     *
     * @param valueClass the intermediate value class
     */

    @Override
    public void setValueClass(Class valueClass) {
        try {
            this.mapValueType = (PropertyGraphElement) valueClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * Set the vertex id class.
     * <p>This can either be a {@code StringType} or {@code LongType}, which are writable encapsulations of the
     * {@code String} and {@code Long} types, respectively. </p>
     * @param vidClass the class of the vertex IDs
     * @see com.intel.hadoop.graphbuilder.types.StringType
     * @see com.intel.hadoop.graphbuilder.types.LongType
     */

    @Override
    public void setVidClass(Class vidClass) {
        this.vidClass = vidClass;
    }

    /**
     * Get the configuration of the current job.
     * @return Hadoop configuration of the current job
     */

    public Configuration getConf() {
        return this.conf;
    }

    /**
     * Set user defined options.
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
     * Run the graph building pipeline.
     *
     * @param cmd user provided command line options
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void run(CommandLine cmd)
            throws IOException, ClassNotFoundException, InterruptedException {

        String outputPath = cmd.getOptionValue("out");

        // Set required parameters in configuration

        String test = tokenizer.getClass().getName();

        conf.set("GraphTokenizer", tokenizer.getClass().getName());
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

        inputConfiguration.updateConfigurationForMapper(conf, cmd);

        // create job from configuration and initialize MR parameters

        Job job = new Job(conf, "TextGraphMR");
        job.setJarByClass(TextGraphMR.class);

        // configure mapper  and input

        inputConfiguration.updateJobForMapper(job, cmd);

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

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // fired up and ready to go!

        LOG.info("=========== Job: Creating vertex list and edge list from input data, saving as text file ===========");

        LOG.info("input: " + inputConfiguration.getDescription());
        LOG.info("Output = " + outputPath);

        LOG.info("InputFormat = " + inputConfiguration.getDescription());
        LOG.info("GraphTokenizerFromString = " + tokenizer.getClass().getName());

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
