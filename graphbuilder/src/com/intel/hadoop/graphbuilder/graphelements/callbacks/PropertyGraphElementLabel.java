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

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.WritableComparable;

/**
 * get the graph element label. if it's an edge it will return the edge label otherwise it will return null
 *
 * @see PropertyGraphElement
 */
public class PropertyGraphElementLabel implements PropertyGraphElementTypeCallback {
    @Override
    public StringType edge(PropertyGraphElement propertyGraphElement, Object... args) {
        Edge edge = (Edge)propertyGraphElement;
        return edge.getEdgeLabel();
    }

    @Override
    public Object vertex(PropertyGraphElement propertyGraphElement, Object... args) {
        return null;
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, Object... args) {
        return null;
    }
}
