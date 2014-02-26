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
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This class reads edges from HDFS by IntermediateEdgeWriterReducer. The
 * Titan ID of both source and target vertices are written along with
 * the edge properties in the previous step. In this map-only job,
 * the map function fetches the reference to the source and target vertices
 * from Titan and adds the edges to Titan using the Blueprints addEdge() API.
 * The edge properties are also written to Titan
 */

public class EdgesIntoTitanMapper extends Mapper<IntWritable,
        SerializedGraphElement, NullWritable, NullWritable> {
    private static final Logger LOG = Logger.getLogger(
            EdgesIntoTitanMapper.class);

    private static enum Counters {
        EDGE_PROPERTIES_WRITTEN,
        NUM_EDGES
    }

    TitanGraph graph;

    /**
     * If appendToExistingGraph then try to find existing graph elements before
     * creating new ones. Enabled with -a command line option.
     */
    private boolean appendToExistingGraph = false;

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
     * @param context  The reducer context provided by Hadoop.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {

        this.graph = getTitanGraphInstance(context);

        appendToExistingGraph = context.getConfiguration().getBoolean(TitanConfig.GRAPHBUILDER_TITAN_APPEND, Boolean.FALSE);
    }

    /**
     * The map function reads the references of the source and target
     * vertices of a given edge and writes the edge and its properties to
     * Titan using the Blueprints addEdge() call
     *
     * @param key The data structure of the input file is every record
     *            per line is a serialized edge. Input of this
     *            map comes from the output directory written by
     *            IntermediateEdgeWriterReducer.java
     * @param serializedGraphElement A serialized edge
     * @param context Hadoop Job context
     */

    @Override
    public void map(IntWritable key, SerializedGraphElement serializedGraphElement,
                    Context context) throws IOException, InterruptedException {

        if (serializedGraphElement.graphElement().isVertex()) {
            // This is a strange case, throw an exception
            throw new IllegalArgumentException("GRAPHBUILDER_ERROR: " +
                    "Found unexpected Vertex element in the edge write " +
                    "mapper. Please recheck the logic to create vertices and " +
                    "edges.");
        }

        Vertex srcBlueprintsVertex =
                    this.graph.getVertex(serializedGraphElement.graphElement
                            ().getProperty(TitanGraphElementWriter
                            .PROPERTY_KEY_SRC_TITAN_ID));
        Vertex tgtBlueprintsVertex =
                    this.graph.getVertex(serializedGraphElement.graphElement
                            ().getProperty(TitanGraphElementWriter
                            .PROPERTY_KEY_TGT_TITAN_ID));
        PropertyMap propertyMap = (PropertyMap) serializedGraphElement
                .graphElement().getProperties();

        // Add the edge to Titan graph
        String edgeLabel = serializedGraphElement.graphElement().getLabel()
                .toString();

        com.tinkerpop.blueprints.Edge bluePrintsEdge = findOrCreateEdge(srcBlueprintsVertex, tgtBlueprintsVertex, edgeLabel);

        // The edge is added to the graph; now add the edge properties.

        // The "srcTitanID" property was added during this MR job to
        // propagate the Titan ID of the edge's source vertex to this
        // reducer ... we can remove it now.

        propertyMap.removeProperty(TitanGraphElementWriter
                .PROPERTY_KEY_SRC_TITAN_ID);
        propertyMap.removeProperty(TitanGraphElementWriter
                .PROPERTY_KEY_TGT_TITAN_ID);

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
     * Find an existing edge in Titan or create a new one
     * @param srcVertex the src for the edge
     * @param tgtVertex the destination for the edge
     * @param label for the edge
     * @return the existing or newly created edge
     */
    protected Edge findOrCreateEdge(Vertex srcVertex, Vertex tgtVertex, String label) {
        Edge bluePrintsEdge = null;
        if (appendToExistingGraph) {
            bluePrintsEdge = findEdge(srcVertex, tgtVertex, label);
        }
        if (bluePrintsEdge == null) {
            bluePrintsEdge = addEdge(srcVertex, tgtVertex, label);
        }
        return bluePrintsEdge;
    }

    /**
     * Find an existing edge in Titan, if it exists, by the supplied parameters
     * @param srcVertex the src for the edge
     * @param dstVertex the destination for the edge
     * @param label of the edge
     * @return the edge if it exists, otherwise null
     */
    protected Edge findEdge(Vertex srcVertex, Vertex dstVertex, String label) {
        Edge bluePrintsEdge = null;
        Iterable<Edge> edges = srcVertex.query().direction(Direction.OUT).labels(label).edges();
        for (Edge edge : edges) {
            if (edge.getVertex(Direction.IN).equals(dstVertex)) {
                bluePrintsEdge = edge;
            }
        }
        return bluePrintsEdge;
    }

    /**
     * Add the edge to Titan
     * @param srcVertex src vertex
     * @param dstVertex destination vertex
     * @param label for the edge
     * @return the newly created edge
     */
    private Edge addEdge(Vertex srcVertex, Vertex dstVertex, String label) {
        try {
            // Major operation - add the edge to Titan graph
            return this.graph.addEdge(null, srcVertex, dstVertex, label);

        } catch (IllegalArgumentException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.TITAN_ERROR,
                    "Could not add edge to Titan; likely a schema error. The label on the edge is  " + label,
                    LOG, e);

            // never happens because of graphBuilderFatalExitException()
            return null;
        }
    }

    /**
     * Performs cleanup tasks after the reducer finishes.
     *
     * In particular, closes the Titan graph.
     * @param context  Hadoop provided reducer context.
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

    /** change flag for testing purposes */
    protected void setAppendToExistingGraph(boolean appendToExistingGraph) {
        this.appendToExistingGraph = appendToExistingGraph;
    }
}
