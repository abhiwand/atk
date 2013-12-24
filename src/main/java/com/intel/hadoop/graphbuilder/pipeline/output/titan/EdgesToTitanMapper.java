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
package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class EdgesToTitanMapper extends Mapper<IntWritable,
        SerializedGraphElement, NullWritable, NullWritable> {
    private static final Logger LOG = Logger.getLogger(
            EdgesToTitanMapper.class);

    private static enum Counters {
        EDGE_PROPERTIES_WRITTEN,
        NUM_EDGES
    }

    TitanGraph graph;

    /*
     * Creates the Titan graph for saving edges and removes the static open
     * method from setup so it can be mocked-up.
     *
     * @return {@code TitanGraph}  For saving edges.
     * @throws IOException
     */
    private TitanGraph getTitanGraphInstance (Context context) throws
            IOException {
        BaseConfiguration titanConfig = new BaseConfiguration();
        return GraphDatabaseConnector.open("titan",
                titanConfig,
                context.getConfiguration());
    }

    /**
     * Sets up the Titan connection.
     *
     * @param {@code context}  The reducer context provided by Hadoop.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {

        this.graph = getTitanGraphInstance(context);
    }

    /**
     * @param key
     */

    @Override
    public void map(IntWritable key, SerializedGraphElement value,
                    Context context) throws IOException, InterruptedException {

        SerializedGraphElement serializedGraphElement = value;

        if (serializedGraphElement.graphElement().isVertex()) {
            // This is a strange case, throw an exception
            throw new IllegalArgumentException("GRAPHBUILDER_ERROR: Recheck " +
                    "the vertex and edge graph elements. Vertex is showing up" +
                    " in the edge mapper to Titan");
        }

        com.tinkerpop.blueprints.Vertex srcBlueprintsVertex =
                    this.graph.getVertex(serializedGraphElement.graphElement
                            ().getProperty("srcTitanID"));
        com.tinkerpop.blueprints.Vertex tgtBlueprintsVertex =
                    this.graph.getVertex(serializedGraphElement.graphElement
                            ().getProperty("tgtTitanID"));
        PropertyMap propertyMap = (PropertyMap) serializedGraphElement
                .graphElement().getProperties();

        // Add the edge to Titan graph

        com.tinkerpop.blueprints.Edge bluePrintsEdge = null;
        String edgeLabel = serializedGraphElement.graphElement().getLabel()
                .toString();
        try {

            bluePrintsEdge = this.graph.addEdge(null,
                        srcBlueprintsVertex,
                        tgtBlueprintsVertex,
                        edgeLabel);

        } catch (IllegalArgumentException e) {

            GraphBuilderExit.graphbuilderFatalExitException(
                        StatusCode.TITAN_ERROR,
                        "Could not add edge to Titan; likely a schema error. " +
                        "The label on the edge is  " + edgeLabel, LOG, e);
        }

        // The edge is added to the graph; now add the edge properties.

        // The "srcTitanID" property was added during this MR job to
        // propagate the Titan ID of the edge's source vertex to this
        // reducer ... we can remove it now.

        propertyMap.removeProperty("srcTitanID");

        for (Writable propertyKey : propertyMap.getPropertyKeys()) {
           EncapsulatedObject mapEntry = (EncapsulatedObject)
                        propertyMap.getProperty(propertyKey.toString());

           try {
               bluePrintsEdge.setProperty(propertyKey.toString(),
                       mapEntry.getBaseObject());
           } catch (IllegalArgumentException e) {
               LOG.fatal("GRAPHBUILDER_ERROR: Could not add edge " +
                            "property; probably a schema error. The label on " +
                            "the edge is  " + edgeLabel);
               LOG.fatal("GRAPHBUILDER_ERROR: The property on the edge " +
                            "is " + propertyKey.toString());
               LOG.fatal(e.getMessage());
               GraphBuilderExit.graphbuilderFatalExitException
                            (StatusCode.INDESCRIBABLE_FAILURE, "", LOG, e);
           }
        }

        context.getCounter(Counters.NUM_EDGES).increment(1L);
    }   // End of map function

    /**
     * Performs cleanup tasks after the reducer finishes.
     *
     * In particular, closes the Titan graph.
     * @param {@code context}  Hadoop provided reducer context.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        this.graph.shutdown();
    }

    public  Enum getEdgeCounter(){
        return Counters.NUM_EDGES;
    }

    public Enum getEdgePropertiesCounter(){
        return Counters.EDGE_PROPERTIES_WRITTEN;
    }
}
