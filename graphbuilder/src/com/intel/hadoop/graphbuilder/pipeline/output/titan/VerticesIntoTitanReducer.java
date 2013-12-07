/**
 * Copyright (C) 2012 Intel Corporation.
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
package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElements;
import com.intel.hadoop.graphbuilder.pipeline.output.rdfgraph.RDFGraphMergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.DestinationVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.thinkaurelius.titan.core.TitanGraph;

import com.intel.hadoop.graphbuilder.util.Functional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.commons.configuration.BaseConfiguration;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;

import org.apache.log4j.Logger;

/**
 * This reducer performs the following tasks:
 * <ul>
 *  <li>duplicate edges and vertices are removed</li>
 *  <li>each vertex is loaded into Titan and is tagged with its Titan ID and passed to the next MR job
 *   through the temp file</li>
 *  <li>each edge is tagged with the Titan ID of its source vertex and passed to the next MR job</li>
 * </ul>
 * <p>
 *  It is expected that the mapper will set keys so that edges are gathered with the source vertices during the shuffle.
 * </p>
 * @see com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction
 */

public class VerticesIntoTitanReducer extends Reducer<IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> {

    private static final Logger LOG = Logger.getLogger(VerticesIntoTitanReducer.class);

    private boolean    noBiDir;
    private Functional edgeReducerFunction;
    private Functional vertexReducerFunction;
    private TitanGraph graph;

    private HashMap<Object, Long>  vertexNameToTitanID;
    private IntWritable            outKey;
    private SerializedPropertyGraphElement outValue;
    private Class                  outClass;

    private final KeyFunction keyFunction = new DestinationVertexKeyFunction();

    private static enum Counters {
        NUM_VERTICES,
        NUM_EDGES
    }

    private PropertyGraphElements propertyGraphElements;

    /**
     * Create the titan graph for saving edges and remove the static open method from setup so it can be mocked
     *
     * @return TitanGraph for saving edges
     * @throws IOException
     */
    private TitanGraph getTitanGraphInstance (Context context) throws IOException {
        BaseConfiguration titanConfig = new BaseConfiguration();
        return GraphDatabaseConnector.open("titan", titanConfig, context.getConfiguration());
    }

    /**
     * Set up the reducer at the start of the task.
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context)  throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        outClass = context.getMapOutputValueClass();
        outKey   = new IntWritable();

        try {
            outValue   = (SerializedPropertyGraphElement) outClass.newInstance();
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Cannot instantiate new reducer output value ( " + outClass.getName() + ")", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Illegal access exception when instantiating reducer output value ( " + outClass.getName() + ")",
                    LOG, e);
        }


        this.vertexNameToTitanID = new HashMap<Object, Long>();

        this.graph = getTitanGraphInstance(context);
        assert (null != this.graph);

        this.noBiDir = conf.getBoolean("noBiDir", false);

        try {
            if (conf.get("edgeReducerFunction") != null) {
                this.edgeReducerFunction =
                        (Functional) Class.forName(conf.get("edgeReducerFunction")).newInstance();

                this.edgeReducerFunction.configure(conf);
            }

            if (conf.get("vertexReducerFunction") != null) {
                this.vertexReducerFunction =
                        (Functional) Class.forName(conf.get("vertexReducerFunction")).newInstance();

                this.vertexReducerFunction.configure(conf);
            }
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Could not instantiate reducer functions", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when instantiating reducer functions", LOG, e);
        } catch (ClassNotFoundException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Class not found exception when instantiating reducer functions", LOG, e);
        } catch (Functional.FunctionalConfigurationError e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Functional error configuring reducer function", LOG, e);
        }

        initPropertyGraphElements(context);
    }

    /**
     * The main reducer routine. Performs duplicate removal followed by vertex load, then a propagation of
     * vertex IDs to the edges whose source is the current vertex.
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(IntWritable key, Iterable<SerializedPropertyGraphElement> values, Context context)
            throws IOException, InterruptedException {

        propertyGraphElements.mergeDuplicates(values);
        propertyGraphElements.write();

    }

    /**
     * Closes the Titan graph connection at the end of the reducer.
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        this.graph.shutdown();
    }

    private void initPropertyGraphElements(Context context){
        propertyGraphElements = new PropertyGraphElements(new RDFGraphMergedGraphElementWrite(),vertexReducerFunction,
                edgeReducerFunction, context, graph, outValue, Counters.NUM_EDGES, Counters.NUM_VERTICES);

    }

    public  Enum getEdgeCounter(){
        return Counters.NUM_EDGES;
    }

    public Enum getVertexCounter(){
        return Counters.NUM_VERTICES;
    }


}
