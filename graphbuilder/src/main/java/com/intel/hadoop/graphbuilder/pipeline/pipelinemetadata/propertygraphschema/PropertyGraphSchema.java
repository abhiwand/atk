/**
 * Copyright (C) 2013 Intel Corporation.
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
package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import com.intel.hadoop.graphbuilder.util.HashUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * The schema or "signature" of a property graph. It contains all the possible
 * types of edges and vertices that it may contain. (It might contain types for
 * edges or vertices that are not witnessed by any element present in the graph.)
 * <p/>
 * <p>
 * The expected use of this information is declaring keys for loading the
 * constructed graph into a graph database. </p>
 */
public class PropertyGraphSchema {

    private HashSet<EdgeSchema> edgeSchemata;
    private HashSet<PropertySchema> propertySchemata;

    /**
     * Allocates a new property graph schema.
     */
    public PropertyGraphSchema() {
        edgeSchemata = new HashSet<>();
        propertySchemata = new HashSet<>();
    }

    /**
     * Allocate and initialize the property graph schema from property type information and edge signatures.
     *
     * @param propTypeMap    Map of property names to datatype classes.
     * @param edgeSignatures Map of edge labels to lists of property names.
     */
    public PropertyGraphSchema(HashMap<String, Class<?>> propTypeMap, HashMap<String, ArrayList<String>> edgeSignatures) {
        edgeSchemata = new HashSet<>();
        propertySchemata = new HashSet<>();

        for (String property : propTypeMap.keySet()) {
            propertySchemata.add(new PropertySchema(property, propTypeMap.get(property)));
        }

        for (String edgeLabel : edgeSignatures.keySet()) {

            EdgeSchema edgeSchema = new EdgeSchema(edgeLabel);

            ArrayList<String> properties = edgeSignatures.get(edgeLabel);

            for (String property : properties) {
                PropertySchema propertySchema = new PropertySchema(property, propTypeMap.get(property));
                edgeSchema.addPropertySchema(propertySchema);
            }

            edgeSchemata.add(edgeSchema);
        }
    }

    /**
     * Gets the property schemas of the property graph.
     *
     * @return A reference to the property graph's property schemas set.
     */
    public HashSet<PropertySchema> getPropertySchemata() {
        return propertySchemata;
    }

    /**
     * Adds an edge schema to the edge schemas of a property graph.
     *
     * @param edgeSchema The edge schema to add.
     */
    public void addEdgeSchema(EdgeSchema edgeSchema) {
        edgeSchemata.add(edgeSchema);
        propertySchemata.addAll(edgeSchema.getPropertySchemata());
    }

    /**
     * Gets the edge schemas of the property graph.
     *
     * @return A reference to the property graph's edge schemas set.
     */
    public HashSet<EdgeSchema> getEdgeSchemata() {
        return edgeSchemata;
    }

    /**
     * Add a property schema to a property graph schema.
     *
     * @param propertySchema The property schema to be added to the property graph schema.
     */
    public void addPropertySchema(PropertySchema propertySchema) {
        propertySchemata.add(propertySchema);
    }

    /**
     * Equality test.
     *
     * @param in Object for comparison.
     * @return {@code true} if and only if the other object is also a {@code PropertyGraphSchema} whose sets of
     *         {@code PropertySchema}'s and {@code EdgeSchema}'s are set-equal to those of this graph schema.
     */
    public boolean equals(Object in) {
        if (in instanceof PropertyGraphSchema) {
            PropertyGraphSchema inGraphSchema = (PropertyGraphSchema) in;

            return ((inGraphSchema.getPropertySchemata().size() == this.getPropertySchemata().size())
                    && (inGraphSchema.getEdgeSchemata().size() == this.getEdgeSchemata().size())
                    && (inGraphSchema.getPropertySchemata().containsAll(this.getPropertySchemata()))
                    && (inGraphSchema.getEdgeSchemata().containsAll(this.getEdgeSchemata())));
        } else {
            return false;
        }
    }

    /**
     * The Java hashCode() method.
     *
     * @return intger hash
     * @see {@code equals}
     */
    public int hashCode() {
        int code = 0;

        for (EdgeSchema edgeSchema : this.getEdgeSchemata()) {
            code = HashUtil.combine(code, edgeSchema.hashCode());
        }

        for (PropertySchema propertySchema : this.getPropertySchemata()) {
            code = HashUtil.combine(code, propertySchema.hashCode());
        }

        return code;
    }
}
