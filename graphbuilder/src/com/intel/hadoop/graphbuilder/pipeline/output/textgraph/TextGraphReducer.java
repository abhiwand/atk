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
package com.intel.hadoop.graphbuilder.pipeline.output.textgraph;

import java.io.IOException;
import java.util.Hashtable;

import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.graphelements.callbacks.GraphElementTypeCallback;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.GraphElementMerge;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphElementWriter;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanGraphElementWriter;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.intel.hadoop.graphbuilder.util.Functional;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

/**
 * The Reducer class applies user defined {@code Functional}s to reduce
 * duplicate edges and vertices. If the user does not provide any {@code Functional}, it
 * outputs the first instance and discards the rest with the same identifier. It
 * also discards self edges: v - > v. An option for discarding bidirectional edges
 * is provided by {@code setCleanBidirectionalEdges(boolean)}.
 * <p>
 * Output directory structure:
 * <ul>
 * <li>$outputdir/edata Contains the edge data output.</li>
 * <li>$outputdir/vdata Contains the vertex data output.</li>
 * </ul>
 * </p>
 */

public class TextGraphReducer extends Reducer<IntWritable, SerializedPropertyGraphElement, NullWritable, Text> {

    private static final Logger LOG = Logger.getLogger(TextGraphReducer.class);

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    private boolean    noBiDir;
    private Functional edgeReducerFunction;
    private Functional vertexReducerFunction;

    private static enum Counters {
        NUM_VERTICES,
        NUM_EDGES
    }

    private Hashtable<EdgeID, Writable> edgeSet;
    private Hashtable<Object, Writable>   vertexSet;

    private GraphElementWriter textWriter;
    private GraphElementTypeCallback graphElementWrite;

    @Override
    public void setup(Context context) {

        Configuration conf = context.getConfiguration();

        this.noBiDir = conf.getBoolean("noBiDir", false);

        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
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
                    "GRAPHBUILDER_ERROR: Could not instantiate reducer functions", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Illegal access exception when instantiating reducer functions", LOG, e);
        } catch (ClassNotFoundException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Class not found exception when instantiating reducer functions", LOG, e);
        } catch (Functional.FunctionalConfigurationError e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Configuration error when configuring reducer functionals.", LOG, e);
        }

        initMergerWriter(context);
    }

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
     * remove duplicate edges/vertices and merge their property maps
     *
     * @param graphElement the graph element to add to our existing vertexSet or edgeSet
     */
    private void merge(Hashtable<EdgeID, Writable> edgeSet, Hashtable<Object, Writable> vertexSet,
                       GraphElement graphElement){
        graphElement.typeCallback(graphElementWrite,
                ArgumentBuilder.newArguments().with("edgeSet", edgeSet).with("vertexSet", vertexSet)
                        .with("edgeReducerFunction", edgeReducerFunction)
                        .with("vertexReducerFunction", vertexReducerFunction)
                        .with("noBiDir", noBiDir));
    }

    /**
     * Call GraphElementWriter function the class  was initiated with to write the edges and vertices.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void write(Hashtable<EdgeID, Writable> edgeSet, Hashtable<Object, Writable> vertexSet,
                      Context context) throws IOException, InterruptedException {

        textWriter.write(ArgumentBuilder.newArguments().with("edgeSet", edgeSet)
                .with("vertexSet", vertexSet).with("vertexCounter", Counters.NUM_VERTICES)
                .with("edgeCounter", Counters.NUM_EDGES).with("context", context)
        );
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    private void initMergerWriter(Context context){
        graphElementWrite = new GraphElementMerge();
        textWriter = new TitanGraphElementWriter();
    }
}
