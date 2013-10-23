
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
 * This reducer loads edges into Titan.
 *
 * It gathers each vertex with the edges that point to that vertex, that is,
 * those edges for whom the vertex is the destination.  Because the edges were tagged with the
 * Titan IDs of their sources in the previous MR job and each vertex is tagged with its Titan ID,
 * we now know the Titan ID of the source and destination of the edges and can add them to Titan.
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
     * create the titan graph for saving edges and remove the static open method from setup so it can be mocked
     *
     * @return TitanGraph for saving edges
     * @throws IOException
     */
    protected TitanGraph tribecaGraphFactoryOpen(Context context) throws IOException {
        BaseConfiguration titanConfig = new BaseConfiguration();
        return GraphDatabaseConnector.open("titan", titanConfig, context.getConfiguration());
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        this.vertexNameToTitanID = new HashMap<Object, Long>();

        this.graph               = tribecaGraphFactoryOpen(context);

        assert (null != this.graph);
    }

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

            com.tinkerpop.blueprints.Edge bluePrintsEdge = this.graph.addEdge(null,
                                                                              srcBlueprintsVertex,
                                                                              tgtBlueprintsVertex,
                                                                              label);
            // Edge is added to the graph; now add the edge properties

            // the "srcTitanID" property was added during this MR job to propagate the Titan ID of the edge's
            // source vertex to this reducer... we can remove it now

            propertyMap.removeProperty("srcTitanID");

            for (Writable propertyKey : propertyMap.getPropertyKeys()) {
                EncapsulatedObject mapEntry = (EncapsulatedObject) propertyMap.getProperty(propertyKey.toString());

                bluePrintsEdge.setProperty(propertyKey.toString(), mapEntry.getBaseObject());
            }

            edgeCount++;
        }

        context.getCounter(Counters.NUM_EDGES).increment(edgeCount);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        this.graph.shutdown();
    }
}
