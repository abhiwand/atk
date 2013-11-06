

package com.intel.hadoop.graphbuilder.graphconstruction.keyfunction;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

public  interface KeyFunction {

    public  int getEdgeKey(Edge edge);
    public  int getVertexKey(Vertex vertex);
}