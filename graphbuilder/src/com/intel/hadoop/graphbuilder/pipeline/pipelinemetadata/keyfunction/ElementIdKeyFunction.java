

package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;


/**
 * Generate key for map reduce by hashing vertices by their ID and edges by their edge ID.
 *
 * @see KeyFunction
 */
public class ElementIdKeyFunction implements KeyFunction {

    /**
     * Generate an integer hash of an edge by hashing its ID
     *
     * @param edge
     * @return  hash code of the  edge ID
     */
    public int getEdgeKey(Edge edge) {
        return edge.getEdgeID().hashCode();
    }

    /**
     * Generate an integer hash of a vertex by hashing its ID
     *
     * @param vertex
     * @return  hash code of the  vertex ID
     */
    public int getVertexKey(Vertex vertex) {
        return vertex.getVertexId().hashCode();
    }
}
