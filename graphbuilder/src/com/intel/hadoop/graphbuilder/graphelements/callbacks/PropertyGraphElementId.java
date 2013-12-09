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


import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import org.apache.hadoop.io.WritableComparable;

/**
 * get the graph elements id
 *
 * @see PropertyGraphElement
 */
public class PropertyGraphElementId implements PropertyGraphElementTypeCallback {
    @Override
    public EdgeID edge(PropertyGraphElement propertyGraphElement, ArgumentBuilder args) {
        Edge edge = (Edge)propertyGraphElement;
        return edge.getEdgeID();
    }

    @Override
    public WritableComparable vertex(PropertyGraphElement propertyGraphElement, ArgumentBuilder  args) {
        Vertex vertex = (Vertex)propertyGraphElement;
        return vertex.getVertexId();
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, ArgumentBuilder args) {
        return null;
    }
}
