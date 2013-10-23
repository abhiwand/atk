

package com.intel.hadoop.graphbuilder.graphconstruction.keyfunction;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public class DestinationVertexKeyFunction  implements KeyFunction {

    public int getEdgeKey(Edge edge) {
        return edge.getDst().hashCode();
    }

    public int getVertexKey(Vertex vertex) {
        return vertex.hashCode();
    }
}