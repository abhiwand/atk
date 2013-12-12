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
package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * The schema or "signature" of a property graph. It contains all the possible types of edges and vertices that it may
 * contain. (It might contain types for edges or vertices that are not witnessed by any element present in the
 * graph.)
 *
 * <p>
 * The expected use of this information is declaring keys for loading the constructed graph into a graph database. </p>
 */
public class PropertyGraphSchema {

    private ArrayList<VertexSchema> vertexSchemata;
    private ArrayList<EdgeSchema>   edgeSchemata;

    /**
     * Allocate a new property graph schema.
     */
    public PropertyGraphSchema() {
        vertexSchemata = new ArrayList<VertexSchema>();
        edgeSchemata   = new ArrayList<EdgeSchema>();
    }

    /**
     * Add a vertex schema to the vertex schemata of a property graph
     * @param vertexSchema
     */
    public void addVertexSchema(VertexSchema vertexSchema) {
        vertexSchemata.add(vertexSchema);
    }

    /**
     * Get the vertex schemata of the property graph.
     * @return  reference to the property graph's vertex schemata list
     */
    public ArrayList<VertexSchema> getVertexSchemata() {
        return vertexSchemata;
    }

    /**
     * Add an edge schema to the edge schemata of a property graph.
     * @param edgeSchema
     */
    public void addEdgeSchema(EdgeSchema edgeSchema) {
        edgeSchemata.add(edgeSchema);
    }

    /**
     * Get the edge schemata of the property graph.
     * @return reference to the property graph's edge schemata list
     */
    public ArrayList<EdgeSchema> getEdgeSchemata() {
        return edgeSchemata;
    }

    /**
     * Obtain a set of the property names used in the schema of the property graph.
     * <p>The set is newly allocated and populated with each call.</p>
     * @return set of strings containing the names of the properties used by the property graph
     */
    public HashMap<String, Class<?>> getMapOfPropertyNamesToDataTypes() {

        HashMap<String, Class<?>> map = new HashMap<String, Class<?>>();

        for (EdgeSchema edgeSchema : edgeSchemata) {
            for (PropertySchema propertySchema : edgeSchema.getPropertySchemata()) {
                map.put(propertySchema.getName(), propertySchema.getType());
            }
        }

        for (VertexSchema vertexSchema : vertexSchemata) {
            for (PropertySchema propertySchema : vertexSchema.getPropertySchemata()) {
                map.put(propertySchema.getName(), propertySchema.getType());
            }
        }
        return map;
    }

}
