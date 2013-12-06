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
 * set the all the vertex or edge properties with the passed PropertyMap
 *
 * @see PropertyGraphElement
 */
public class PropertyGraphElementSetProperties implements PropertyGraphElementTypeCallback {
    private PropertyMap propertyMap;

    @Override
    public <T> T edge(PropertyGraphElement propertyGraphElement, Object... args) {
        initArguments(args);

        Edge edge = (Edge)propertyGraphElement;
        edge.setProperties(propertyMap);
        return null;
    }

    @Override
    public <T> T vertex(PropertyGraphElement propertyGraphElement, Object... args) {
        initArguments(args);

        Vertex vertex = (Vertex)propertyGraphElement;
        vertex.setProperties(propertyMap);
        return null;
    }

    @Override
    public <T> T nullElement(PropertyGraphElement propertyGraphElement, Object... args) {
        return null;
    }

    /**
     * get the property map variable
     *
     * @param args variable length arguments
     */
    private void initArguments(Object ... args){
        if(args.length == 1){
            propertyMap = (PropertyMap) args[0];
        }else{
            throw new IllegalArgumentException("Incorrect number of arguments expect exactly 1 given: " + args.length);
        }
    }
}
