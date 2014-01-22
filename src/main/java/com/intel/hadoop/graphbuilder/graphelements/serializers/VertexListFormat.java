/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.graphelements.serializers;

import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;

/**
 * Converts a Vertex to the VertexListFormat and then back into a Vertex.
 *
 * Use for reading/writing vertex lists.
 */
public class VertexListFormat {

    private final String delimiter;

    /**
     * Converts a Vertex to the VertexListFormat and then back into a Vertex.
     */
    public VertexListFormat() {
        this.delimiter = "\t";
    }

    /**
     * Converts a Vertex to the VertexListFormat and then back into a Vertex.
     *
     * @param delimiter in case you want to use something other than the tab character
     */
    public VertexListFormat(String delimiter) {
        this.delimiter = delimiter;
    }

    /**
     * Convert a Vertex to the VertexListFormat
     * @param vertex to convert
     * @param withProperties true if properties should be included in the serialization
     * @return the vertex in VertexListFormat
     */
    public String toString(Vertex vertex, boolean withProperties) {
        if (withProperties) {
            return toStringWithProperties(vertex);
        }
        else {
            return toStringWithoutProperties(vertex);
        }
    }

    /**
     * Convert a Vertex to the VertexListFormat.
     *
     * Properties are included in the serialization.
     *
     * @param vertex to convert
     * @return the vertex in VertexListFormat
     */
    protected String toStringWithProperties(Vertex vertex) {
        String vertexString = toStringWithoutProperties(vertex);
        if (vertex.getProperties() != null) {
            PropertyMap propertyMap = vertex.getProperties();
            for (Writable key : propertyMap.getPropertyKeys()) {
                vertexString += delimiter + key + delimiter + propertyMap.getProperty(key.toString());
            }
        }
        return vertexString;

    }

    /**
     * Convert a Vertex to the VertexListFormat.
     *
     * Properties are NOT included in the serialization.
     *
     * @param vertex to convert
     * @return the vertex in VertexListFormat
     */
    protected String toStringWithoutProperties(Vertex vertex) {
        String vertexString = vertex.getId().getName().toString();
        if (vertex.getLabel() != null) {
            vertexString += delimiter + vertex.getLabel().toString();
        }
        return vertexString;
    }

    /**
     * Convert from VertexListFormat back into a Vertex
     * @param s the Vertex in VertexListFormat
     * @param withProperties include properties when de-serializing
     * @return the de-serialized Vertex
     */
    public Vertex toVertex(String s, boolean withProperties) {

        if (StringUtils.isBlank(s)) {
            throw new IllegalArgumentException("Vertex string can't be blank: " + s);
        }

        Vertex vertex = null;
        String[] parts = s.split(delimiter);
        if (parts.length > 0) {
            vertex = new Vertex<StringType>(new StringType(parts[0]));
            if (parts.length > 1){
                String label = parts[1];
                if (StringUtils.isNotBlank(label)) {
                    vertex.setLabel(new StringType(parts[1]));
                }
                if (withProperties) {
                    PropertyMap propertyMap = new PropertyMap();
                    for (int i = 2; i < parts.length; i = i + 2) {
                        propertyMap.setProperty(parts[i], new StringType(parts[i+1]));
                    }
                    vertex.setProperties(propertyMap);
                }
            }
        }
        return vertex;
    }

}