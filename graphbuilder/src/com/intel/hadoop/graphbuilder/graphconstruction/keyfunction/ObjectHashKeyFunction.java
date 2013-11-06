

package com.intel.hadoop.graphbuilder.graphconstruction.keyfunction;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public class ObjectHashKeyFunction implements KeyFunction {

    public int getEdgeKey(Edge edge) {
        return edge.hashCode();
    }

    public int getVertexKey(Vertex vertex) {
        return vertex.hashCode();
    }
}
