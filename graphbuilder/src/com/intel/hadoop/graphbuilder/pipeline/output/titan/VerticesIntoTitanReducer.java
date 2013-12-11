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

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.callbacks.GraphElementTypeCallback;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.GraphElementMerge;
import com.intel.hadoop.graphbuilder.pipeline.output.MergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.DestinationVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.util.*;
import com.thinkaurelius.titan.core.TitanGraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.commons.configuration.BaseConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;

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

    private Hashtable<Object, Long> vertexNameToTitanID;
    private IntWritable            outKey;
    private SerializedPropertyGraphElement outValue;
    private Class                  outClass;

    private final KeyFunction keyFunction = new DestinationVertexKeyFunction();

    private static enum Counters {
        NUM_VERTICES,
        NUM_EDGES
    }

    private Hashtable<EdgeID, Writable> edgeSet;
    private Hashtable<Object, Writable>   vertexSet;

    private MergedGraphElementWrite titanMergedWrite;
    private GraphElementTypeCallback propertyGraphElementMerge;

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


        this.vertexNameToTitanID = new Hashtable<Object, Long>();

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

        initMergerWriter(context);
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

        edgeSet       = new Hashtable<>();
        vertexSet     = new Hashtable<>();

        for(SerializedPropertyGraphElement serializedPropertyGraphElement: values){
            GraphElement graphElement = serializedPropertyGraphElement.graphElement();

            if(graphElement.isNull()){
                continue;
            }

            //try to add the graph element to the existing set of vertices or edges
            //GraphElementMerge will take care of switching between edge and vertex
            merge(edgeSet, vertexSet, graphElement);
        }

        write(edgeSet, vertexSet, context);
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

    /**
     * remove duplicate edges/vertices and merge their property maps
     *
     * @param graphElement the graph element to add to our existing vertexSet or edgeSet
     */
    private void merge(Hashtable<EdgeID, Writable> edgeSet, Hashtable<Object, Writable> vertexSet,
                       GraphElement graphElement){
        graphElement.typeCallback(propertyGraphElementMerge,
                ArgumentBuilder.newArguments().with("edgeSet", edgeSet).with("vertexSet", vertexSet)
                        .with("edgeReducerFunction", edgeReducerFunction)
                        .with("vertexReducerFunction", vertexReducerFunction)
                        .with("noBiDir", noBiDir));
    }

    /**
     * Call MergedGraphElementWrite function the class  was initiated with to write the edges and vertices.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void write( Hashtable<EdgeID, Writable> edgeSet, Hashtable<Object, Writable> vertexSet, Context context) throws
            IOException, InterruptedException {

        titanMergedWrite.write(ArgumentBuilder.newArguments().with("edgeSet", edgeSet)
                .with("vertexSet", vertexSet).with("vertexCounter", Counters.NUM_VERTICES)
                .with("edgeCounter", Counters.NUM_EDGES).with("context", context)
                .with("graph", graph).with("outValue", outValue).with("outKey", outKey).with("keyFunction", keyFunction));
    }

    private void initMergerWriter(Context context){
        propertyGraphElementMerge = new GraphElementMerge();
        titanMergedWrite = new TitanMergedGraphElementWrite();
    }

    public  Enum getEdgeCounter(){
        return Counters.NUM_EDGES;
    }

    public Enum getVertexCounter(){
        return Counters.NUM_VERTICES;
    }


}
