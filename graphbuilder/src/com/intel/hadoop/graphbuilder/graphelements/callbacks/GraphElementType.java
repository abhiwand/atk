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
package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;

/**
 * return the type of the graph element. Very use full when you don't care if it's an edge or vertex you just want a
 * type
 *
 * @see com.intel.hadoop.graphbuilder.graphelements.GraphElement
 */
public class GraphElementType implements GraphElementTypeCallback {
    public enum GraphType {EDGE, VERTEX}

    /**
     * returns the edge enum type. Although the graphElement and arguments are not used here they must be part of the
     * method definition to satisfy the implementation.
     *
     * @param graphElement graph element to perform operations on
     * @param arguments any arguments that might have been passed
     * @return Edge enum type
     */
    @Override
    public GraphType edge(GraphElement graphElement, ArgumentBuilder arguments) {
        return GraphType.EDGE;
    }

    /**
     * returns the vertex enum type. Although the graphElement and arguments are not used here they must be part of the
     * method definition to satisfy the implementation.
     *
     * @param graphElement graph element to perform operations on
     * @param arguments any arguments that might have been passed
     * @return Vertex enum type
     */
    @Override
    public GraphType vertex(GraphElement graphElement, ArgumentBuilder arguments) {
        return GraphType.VERTEX;
    }
}
