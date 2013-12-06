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
import com.intel.hadoop.graphbuilder.types.PropertyMap;

/**
 * get the graph element properties. although the edge and vertex classes already have the method available this interface
 * saves us from knowing typecasting to an edge or vertex to get the properties.
 *
 * @see PropertyGraphElement
 */
public class PropertyGraphElementGetProperties implements PropertyGraphElementTypeCallback {
    @Override
    public PropertyMap edge(PropertyGraphElement propertyGraphElement, Object ... args) {
        return ((Edge)propertyGraphElement.get()).getProperties();
    }

    @Override
    public PropertyMap vertex(PropertyGraphElement propertyGraphElement, Object ... args) {
        return ((Vertex)propertyGraphElement.get()).getProperties();
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, Object ... args) {
        return null;
    }
}
