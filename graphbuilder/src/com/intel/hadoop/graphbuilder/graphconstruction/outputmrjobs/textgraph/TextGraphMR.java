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

import com.intel.hadoop.graphbuilder.graphconstruction.keyfunction.ObjectHashKeyFunction;
import com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.InputConfiguration;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.util.GraphbuilderExit;
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
 * This MapReduce Job creates an initial edge list and vertex list from raw
 * input data, e.g. text xml. The result set of graph elements does not contain self edges or
 * duplicate vertices or edges.
 * <p>
 * The Mapper class parse each input value, provided by the {@code InputFormat},
 * and output a list of {@code Vertex} and a list of {@code Edge} using a
 * {@code GraphTokenizerFromString}.
 * </p>
 * <p>
 * The Reducer class applies user defined {@code Functional}s to reduce
 * duplicate edges and vertices. If no such {@code Functional} is provide, it
 * outputs the first instance and discards the rest with the same identifier. It
 * also discards self edges: v - > v. An option for discard bidirectional edge
 * is provided by {@code setCleanBidirectionalEdges(boolean)}.
 * </p>
 * <p>
 * Input directory: Can take multiple input directories. Output directory
 * structure:
 * <ul>
 * <li>$outputdir/edata contains edge data output</li>
 * <li>$outputdir/vdata contains vertex data output</li>
 * </ul>
 * </p>
 *
 * @see com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer
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

    private final Class          keyFuncClass = ObjectHashKeyFunction.class;

    private Functional vertexReducerFunction;
    private Functional edgeReducerFunction;
    private boolean    cleanBidirectionalEdge;


    /**
     * set tokenizer and inputconfiguration.
     *
     * @param tokenizer
     * @param inputConfiguration
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
                GraphbuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_CONNECT_TO_HBASE,
                        "Cannot allocate the HBaseUtils object. Check hbase connection.", LOG, e);
            }
            this.conf       = hbaseUtils.getConfiguration();
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
            GraphbuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Cannot instantiate reducer functions.", LOG, e);
        } catch (IllegalAccessException e) {
            GraphbuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
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
            GraphbuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Cannot set value class ( " + valueClass.getName() + ")", LOG, e);
        } catch (IllegalAccessException e) {
            GraphbuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
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
     *
     * run the map reduce job to create a text graph
     *
     * @param cmd         the command line
     *
     *  exceptions are caught by the callee, @code AbstractCreateGraphJob
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void run(CommandLine cmd) throws IOException, ClassNotFoundException, InterruptedException {

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
