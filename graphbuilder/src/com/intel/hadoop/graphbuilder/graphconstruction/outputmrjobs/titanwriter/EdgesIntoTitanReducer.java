
package com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.titanwriter;

import com.intel.hadoop.graphbuilder.graphconstruction.EdgeKey;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Load edges into Titan.
 * <p>
 * It gathers each vertex with the edges that point to that vertex, that is,
 * those edges for whom the vertex is the destination.  Because the edges were tagged with the
 * Titan IDs of their sources in the previous MR job and each vertex is tagged with its Titan ID,
 * we now know the Titan ID of the source and destination of the edges and can add them to Titan.
 * </p>
 */

public class EdgesIntoTitanReducer extends Reducer<IntWritable, PropertyGraphElement, IntWritable, PropertyGraphElement> {
    private static final Logger LOG = Logger.getLogger(EdgesIntoTitanReducer.class);
    private TitanGraph            graph;
    private HashMap<Object, Long> vertexNameToTitanID;

    //  final KeyFunction keyfunction = new SourceVertexKeyFunction();

    private static enum Counters {
        EDGE_PROPERTIES_WRITTEN,
        NUM_EDGES
    }

    /**
     * Create the titan graph for saving edges and remove the static open method from setup so it can be mocked
     *
     * @return TitanGraph for saving edges
     * @throws IOException
     */
    private TitanGraph tribecaGraphFactoryOpen(Context context) throws IOException {
        BaseConfiguration titanConfig = new BaseConfiguration();
        return GraphDatabaseConnector.open("titan", titanConfig, context.getConfiguration());
    }

    /**
     * Set up Titan connection.
     *
     * @param context  the reducer context provided by Hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        this.vertexNameToTitanID = new HashMap<Object, Long>();

        this.graph               = tribecaGraphFactoryOpen(context);
    }

    /**
     * Hadoop-called routine for loading edges into Titan.
     * <p>
     * It is assumed that edges and vertices have been gathered so that every
     * edge shares the reducer of its destination vertex, and that every edge has previously
     * been assigned the TitanID of its source vertex.
     * </p>
     * <p>
     * Titan IDs are propagatd from the destination vertices to each edge and the edges are loaded into Titan
     * using the BluePrints API
     * </p>
     * @param key    mapreduce key; a hash of a vertex ID
     * @param values  either a vertex with that hashed vertex ID, or an edge with said vertex as its destination
     * @param context  reducer context provided by Hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(IntWritable key, Iterable<PropertyGraphElement> values, Context context)
            throws IOException, InterruptedException {

        HashMap<EdgeKey, Writable> edgePropertyTable  = new HashMap();

        Iterator<PropertyGraphElement> valueIterator = values.iterator();

        while (valueIterator.hasNext()) {

            PropertyGraphElement nextElement = valueIterator.next();

            // Apply reduce on vertex

            if (nextElement.graphElementType() == PropertyGraphElement.GraphElementType.VERTEX) {

                Vertex vertex = nextElement.vertex();

                Object      vertexId      = vertex.getVertexId();
                PropertyMap propertyMap   = vertex.getProperties();
                long        vertexTitanId = ((LongType) propertyMap.getProperty("TitanID")).get();

                vertexNameToTitanID.put(vertexId, vertexTitanId);

            } else {

                // Apply reduce on edges, remove self and (or merge) duplicate edges.
                // Optionally remove bidirectional edge.

                Edge<?> edge    = nextElement.edge();
                EdgeKey edgeKey = new EdgeKey(edge.getSrc(), edge.getDst(), edge.getEdgeLabel());

                edgePropertyTable.put(edgeKey, edge.getProperties());
            }
        }

        int edgeCount   = 0;

        // Output edge records

        Iterator<Map.Entry<EdgeKey, Writable>> edgeIterator = edgePropertyTable.entrySet().iterator();

        while (edgeIterator.hasNext()) {

            Map.Entry<EdgeKey, Writable> edgeMapEntry = edgeIterator.next();

            Object dst   = edgeMapEntry.getKey().getDst();
            String label = edgeMapEntry.getKey().getLabel().toString();

            PropertyMap propertyMap = (PropertyMap) edgeMapEntry.getValue();

            long srcTitanId = ((LongType) propertyMap.getProperty("srcTitanID")).get();
            long dstTitanId = vertexNameToTitanID.get(dst);

            com.tinkerpop.blueprints.Vertex srcBlueprintsVertex = this.graph.getVertex(srcTitanId);
            com.tinkerpop.blueprints.Vertex tgtBlueprintsVertex = this.graph.getVertex(dstTitanId);

            // Major operation - add the edge to Titan graph

            com.tinkerpop.blueprints.Edge bluePrintsEdge = null;
            try {
                bluePrintsEdge = this.graph.addEdge(null,
                                                                              srcBlueprintsVertex,
                                                                              tgtBlueprintsVertex,
                                                                              label);
            } catch (IllegalArgumentException e) {
                LOG.fatal("Could not add edge to Titan; likely a schema error. The label on the edge is  " + label);
                System.exit(1);
            }

            // Edge is added to the graph; now add the edge properties

            // the "srcTitanID" property was added during this MR job to propagate the Titan ID of the edge's
            // source vertex to this reducer... we can remove it now

            propertyMap.removeProperty("srcTitanID");

            for (Writable propertyKey : propertyMap.getPropertyKeys()) {
                EncapsulatedObject mapEntry = (EncapsulatedObject) propertyMap.getProperty(propertyKey.toString());

                try {
                bluePrintsEdge.setProperty(propertyKey.toString(), mapEntry.getBaseObject());
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                    e.getMessage();
                    LOG.fatal("Could not add edge property; probably a schema error. The label on the edge is  " + label);
                    LOG.fatal("The property on the edge is " + propertyKey.toString());
                    System.exit(1);
                }

            }

            edgeCount++;
        }

        context.getCounter(Counters.NUM_EDGES).increment(edgeCount);
    }

    /**
     * Perform cleanup tasks after the reducer is finished.
     *
     * In particular, close the Titan graph.
     * @param context    Hadoop provided reducer context.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        this.graph.shutdown();
    }
}
