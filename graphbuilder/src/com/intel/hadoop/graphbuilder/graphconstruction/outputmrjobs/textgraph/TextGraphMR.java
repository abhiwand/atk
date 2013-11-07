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
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphBuildingRule;
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
 * Create a text graph (text edge list and vertex list) from the property graph elements.
 *
 * <p>
 * The Reducer class applies user defined {@code Functional}s to reduce
 * duplicate edges and vertices. If no such {@code Functional} is provide, it
 * outputs the first instance and discards the rest with the same identifier. It
 * also discards self edges: v - > v. An option for discard bidirectional edge
 * is provided by {@code setCleanBidirectionalEdges(boolean)}.
 * </p>
 * <p>
 * Output directorystructure:
 * <ul>
 * <li>$outputdir/edata contains edge data output</li>
 * <li>$outputdir/vdata contains vertex data output</li>
 * </ul>
 * </p>
 *
 * @see TextGraphReducer
 */

public class TextGraphMR extends GraphGenerationMRJob {

    private static final Logger LOG = Logger.getLogger(TextGraphMR.class);

    private Configuration conf;

    private HBaseUtils hbaseUtils = null;
    private boolean    usingHBase = false;

    private GraphBuildingRule  graphBuildingRule;
    private InputConfiguration inputConfiguration;

    private PropertyGraphElement mapValueType;
    private Class                vidClass;

    private final Class          keyFuncClass = ObjectHashKeyFunction.class;

    private Functional vertexReducerFunction;
    private Functional edgeReducerFunction;
    private boolean    cleanBidirectionalEdge;


    /**
     * Set-up time routine that connects raw data ({@code inputConfiguration} and the graph generations rule
     * ({@code graphBuildingRule}) into the MR chain..
     *
     * @param inputConfiguration object that handles the generation of data records from raw data
     * @param graphBuildingRule object that handles the conversion of data records into property graph elements
     */
    @Override
    public void init(InputConfiguration inputConfiguration, GraphBuildingRule graphBuildingRule) {

        this.graphBuildingRule  = graphBuildingRule;
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
     * Set user defined function for reduce duplicate vertex and edges.
     *
     * @param vertexReducerFunction user specified function for reducing duplicate vertices
     * @param edgeReducerFunction   user specified functino for reducing duplicate edges
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
     * Set the option for whether to clean bidirectional edges.
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
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
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
     * Execute the map reduce chain that generates a TextGraph from the previously specified
     * raw input using the previously specified graph building rule.
     *
     * @param cmd     user specified command line
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void run(CommandLine cmd)
            throws IOException, ClassNotFoundException, InterruptedException {

        String outputPath = cmd.getOptionValue("out");

        // Set required parameters in configuration

        String test = graphBuildingRule.getClass().getName();

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

        inputConfiguration.updateConfigurationForMapper(conf, cmd);

        // update the configuration per the graphBuildingRule

        graphBuildingRule.updateConfigurationForTokenizer(conf, cmd);

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
