

package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

/**
 * Generates a key for map reduce by hashing the vertices by their ID and edges by the IDs of their destination vertex.
 *
 * @see KeyFunction
 */
public class SourceVertexKeyFunction  implements KeyFunction {

    /**
     * Generates an integer hash of an edge using its destination vertex.
     *
     * @param edge
     * @return  hash code of the edge's destination vertex ID
     */
    public int getEdgeKey(Edge edge) {
        return edge.getSrc().hashCode();
    }

    /**
     * Generates an integer hash of a vertex by hashing its ID.
     *
     * @param vertex
     * @return  hash code of the  vertex ID
     */

    public int getVertexKey(Vertex vertex) {
        return vertex.getVertexId().hashCode();
    }
}