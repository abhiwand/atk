/**
 * Copyright (C) 2012 Intel Corporation.
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
package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

/**
 * Generate key for map reduce by hashing vertices by their ID and edges by the ID of their destination vertex.
 *
 * @see KeyFunction
 */
public class SourceVertexKeyFunction  implements KeyFunction {

    /**
     * Generate an integer hash of an edge using its destination vertex
     *
     * @param edge
     * @return  hash code of the edge's destination vertex ID
     */
    public int getEdgeKey(Edge edge) {
        return edge.getSrc().hashCode();
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