
package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.DestinationVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanGraph;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.util.Functional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.commons.configuration.BaseConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import com.intel.hadoop.graphbuilder.types.LongType;
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

public class VerticesIntoTitanReducer extends Reducer<IntWritable, PropertyGraphElement, IntWritable, PropertyGraphElement> {

    private static final Logger LOG = Logger.getLogger(VerticesIntoTitanReducer.class);

    private boolean    noBiDir;
    private Functional edgeReducerFunction;
    private Functional vertexReducerFunction;
    private TitanGraph graph;

    private HashMap<Object, Long>  vertexNameToTitanID;
    private IntWritable            outKey;
    private PropertyGraphElement   outValue;
    private Class                  outClass;

    private final KeyFunction keyFunction = new DestinationVertexKeyFunction();

    private static enum Counters {
        NUM_VERTICES,
        NUM_EDGES
    }

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
            outValue   = (PropertyGraphElement) outClass.newInstance();
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
    public void reduce(IntWritable key, Iterable<PropertyGraphElement> values, Context context)
            throws IOException, InterruptedException {

        HashMap<EdgeID, Writable>      edgeSet       = new HashMap();
        HashMap<Object, Writable>      vertexSet     = new HashMap();
        Iterator<PropertyGraphElement> valueIterator = values.iterator();

        while (valueIterator.hasNext()) {

            PropertyGraphElement next = valueIterator.next();

            // Apply reduce on vertex

            if (next.graphElementType() == PropertyGraphElement.GraphElementType.VERTEX) {

                Object vid = next.vertex().getVertexId();

                if (vertexSet.containsKey(vid)) {

                    // vid denotes a duplicate vertex

                    if (vertexReducerFunction != null) {
                        vertexSet.put(vid,
                                vertexReducerFunction.reduce(next.vertex().getProperties(),
                                vertexSet.get(vid)));
                    } else {

                        /**
                         * default behavior is to merge the property maps of duplicate vertices
                         * conflicting key/value pairs get overwritten
                         */

                        PropertyMap existingPropertyMap = (PropertyMap) vertexSet.get(vid);
                        existingPropertyMap.mergeProperties(next.vertex().getProperties());
                    }
                } else {

                    // vid denotes a NON-duplicate vertex

                    if (vertexReducerFunction != null) {
                        vertexSet.put(vid,
                                vertexReducerFunction.reduce(next.vertex().getProperties(),
                                        vertexReducerFunction.identityValue()));
                    } else {
                        vertexSet.put(vid, next.vertex().getProperties());
                    }
                }
            } else {

                // Apply reduce on edges, remove self and (or merge) duplicate edges.
                // Optionally remove bidirectional edge.

                Edge<?> edge    = next.edge();
                EdgeID edgeID = new EdgeID(edge.getSrc(), edge.getDst(), edge.getEdgeLabel());

                if (edge.isSelfEdge()) {
                    // self edges are omitted
                    continue;
                }

                if (edgeSet.containsKey(edgeID)) {

                    // edge is a duplicate

                    if (edgeReducerFunction != null) {
                        edgeSet.put(edgeID, edgeReducerFunction.reduce(edge.getProperties(), edgeSet.get(edgeID)));
                    } else {

                        /**
                         * default behavior is to merge the property maps of duplicate edges
                         * conflicting key/value pairs get overwritten
                         */

                        PropertyMap existingPropertyMap = (PropertyMap) edgeSet.get(edgeID);
                        existingPropertyMap.mergeProperties(edge.getProperties());
                    }
                } else {

                    // edge is a NON-duplicate

                    if (noBiDir && edgeSet.containsKey(edgeID.reverseEdge())) {
                        // in this case, skip the bi-directional edge
                    } else {

                        // edge is either not bi-directional, or we are keeping bi-directional edges

                        if (edgeReducerFunction != null) {
                            edgeSet.put(edgeID,
                                    edgeReducerFunction.reduce(edge.getProperties(),
                                            edgeReducerFunction.identityValue()));
                        } else {
                            edgeSet.put(edgeID, edge.getProperties());
                        }
                    }
                }
            }
        }

        int vertexCount = 0;
        int edgeCount   = 0;

        // Output vertex records

        Iterator<Map.Entry<Object, Writable>> vertexIterator = vertexSet.entrySet().iterator();


        while (vertexIterator.hasNext()) {

            Map.Entry v     = vertexIterator.next();

            // Major operation - vertex is added to Titan and a new ID is assigned to it

            com.tinkerpop.blueprints.Vertex  bpVertex = graph.addVertex(null);

            bpVertex.setProperty(TitanConfig.GB_ID_FOR_TITAN, v.getKey().toString());

            PropertyMap propertyMap = (PropertyMap) v.getValue();

            for (Writable keyName : propertyMap.getPropertyKeys()) {
                EncapsulatedObject mapEntry = (EncapsulatedObject) propertyMap.getProperty(keyName.toString());

                bpVertex.setProperty(keyName.toString(), mapEntry.getBaseObject());
            }

            long vertexId = ((TitanElement) bpVertex).getID();

            Vertex vertex = new Vertex();
            propertyMap.setProperty("TitanID", new LongType(vertexId));
            vertex.configure((WritableComparable) v.getKey(), propertyMap);

            outValue.init(PropertyGraphElement.GraphElementType.VERTEX, vertex);
            outKey.set(keyFunction.getVertexKey(vertex));

            context.write(outKey, outValue);

            vertexNameToTitanID.put(v.getKey(), vertexId);

            vertexCount++;
        }

        context.getCounter(Counters.NUM_VERTICES).increment(vertexCount);

        // Output edge records

        Iterator<Map.Entry<EdgeID, Writable>> edgeIterator = edgeSet.entrySet().iterator();


        while (edgeIterator.hasNext()) {

            Map.Entry<EdgeID, Writable> e = edgeIterator.next();

            Object src                  = e.getKey().getSrc();
            Object dst                  = e.getKey().getDst();
            String label                = e.getKey().getLabel().toString();

            PropertyMap propertyMap = (PropertyMap) e.getValue();

            long srcTitanId = vertexNameToTitanID.get(src);

            Edge edge = new Edge();

            propertyMap.setProperty("srcTitanID", new LongType(srcTitanId));

            edge.configure((WritableComparable)  src, (WritableComparable)  dst, new StringType(label), propertyMap);

            outValue.init(PropertyGraphElement.GraphElementType.EDGE, edge);
            outKey.set(keyFunction.getEdgeKey(edge));

            context.write(outKey, outValue);

            edgeCount++;
        }

        context.getCounter(Counters.NUM_EDGES).increment(edgeCount);
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
}
