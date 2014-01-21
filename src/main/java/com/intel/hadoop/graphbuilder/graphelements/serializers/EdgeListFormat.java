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

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.VertexID;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;

/**
 * Converts a Edge to the EdgeListFormat and then back into a Edge.
 *
 * Use for reading/writing edge lists.
 */
public class EdgeListFormat {

    private final String delimiter;

    /**
     * Converts a Edge to the EdgeListFormat and then back into a Edge.
     */
    public EdgeListFormat() {
        this.delimiter = "\t";
    }

    /**
     * Converts a Edge to the EdgeListFormat and then back into a Edge.
     *
     * @param delimiter in case you want to use something other than the tab character
     */
    public EdgeListFormat(String delimiter) {
        this.delimiter = delimiter;
    }

    /**
     * Convert a Edge to the EdgeListFormat
     * @param edge
     * @param withProperties true if properties should be included in the serialization
     * @return the edge in EdgeListFormat
     */
    public String toString(Edge edge, boolean withProperties) {
        if (withProperties) {
            return toStringWithProperties(edge);
        }
        else {
            return toStringWithoutProperties(edge);
        }
    }

    /**
     * Convert an Edge to the EdgeListFormat.
     *
     * Properties are included in the serialization.
     *
     * @param edge to convert
     * @return the edge in EdgeListFormat
     */
    public String toStringWithProperties(Edge edge) {
        String edgeString = toStringWithoutProperties(edge);
        if (edge.getProperties() != null) {
            PropertyMap propertyMap = edge.getProperties();
            for (Writable key : propertyMap.getPropertyKeys()) {
                edgeString += delimiter + key + delimiter + propertyMap.getProperty(key.toString());
            }
        }
        return edgeString;
    }

    /**
     * Convert an Edge to the EdgeListFormat.
     *
     * Properties are NOT included in the serialization.
     *
     * @param edge to convert
     * @return the edge in EdgeListFormat
     */
    public String toStringWithoutProperties(Edge edge) {
        // TODO: probably shouldn't use toString() here
        return edge.getSrc().toString() + delimiter +
                edge.getDst().toString() + delimiter +
                edge.getLabel().toString();
    }

    /**
     * Convert from EdgeListFormat back into an Edge
     * @param s the Edge in EdgeListFormat
     * @param withProperties include properties when de-serializing
     * @return the de-serialized Edge
     */
    public Edge toEdge(String s, boolean withProperties) {

        if (StringUtils.isBlank(s)) {
            throw new IllegalArgumentException("Edge string can't be blank: " + s);
        }

        Edge edge = null;
        String[] parts = s.split(delimiter);
        if (parts.length > 2) {
            edge = new Edge(parseVertexID(parts[0]), parseVertexID(parts[1]), new StringType(parts[2]));
            if (withProperties) {
                PropertyMap propertyMap = new PropertyMap();
                for (int i = 3; i < parts.length; i = i + 2) {
                    propertyMap.setProperty(parts[i], new StringType(parts[i+1]));
                }
                edge.setProperties(propertyMap);
            }
        }
        return edge;
    }

    /**
     * Expected format is either "label.name" or "name".
     */
    protected VertexID<StringType> parseVertexID(String labelDotName) {

        if (StringUtils.contains(labelDotName, ".")) {
            String name = StringUtils.substringBeforeLast(labelDotName, ".");
            String label = StringUtils.substringAfterLast(labelDotName, ".");
            return new VertexID<>(new StringType(label), new StringType(name));
        }
        else {
            // no label
            return new VertexID<>(new StringType(labelDotName));
        }
    }

}
