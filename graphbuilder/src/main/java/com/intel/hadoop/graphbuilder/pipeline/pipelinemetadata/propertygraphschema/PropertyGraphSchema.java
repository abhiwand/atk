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

import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * The schema or "signature" of a property graph. It contains all the possible 
 * types of edges and vertices that it may contain. (It might contain types for 
 * edges or vertices that are not witnessed by any element present in the graph.)
 *
 * <p>
 * The expected use of this information is declaring keys for loading the 
 * constructed graph into a graph database. </p>
 */
public class PropertyGraphSchema {

    private static final Logger LOG = Logger.getLogger(PropertyGraphSchema.class);

    private ArrayList<VertexSchema> vertexSchemata;
    private ArrayList<EdgeSchema>   edgeSchemata;
    private HashMap<String, Class<?>> propTypeMap;


    /**
     * Allocates a new property graph schema.
     */

    public PropertyGraphSchema() {
        vertexSchemata = new ArrayList<>();
        edgeSchemata   = new ArrayList<>();
        this.propTypeMap = new HashMap<>();
    }

    /**
     * Allocate and initialize the property graph schema from property type information and edge signatures.
     * @param propTypeMap  Map of property names to datatype classes.
     * @param edgeSignatures  Map of edge labels to lists of property names.
     */
    public PropertyGraphSchema(HashMap<String, Class<?>> propTypeMap, HashMap<String, ArrayList<String>> edgeSignatures) {
        vertexSchemata = new ArrayList<>();
        edgeSchemata   = new ArrayList<>();
        this.propTypeMap = propTypeMap;

        for (String edgeLabel : edgeSignatures.keySet()) {

            EdgeSchema edgeSchema = new EdgeSchema(edgeLabel);

            ArrayList<String> properties = edgeSignatures.get(edgeLabel);

            for (String property : properties) {

                // this is horrible, but I won't clean it up until I can safely get rid of the old code
                // the "schema" information should contain property names to datatypes, and edge labels to lists of names
                // with no datatype
                PropertySchema propertySchema = new PropertySchema(property, propTypeMap.get(property));
                edgeSchema.addPropertySchema(propertySchema);
            }

            edgeSchemata.add(edgeSchema);
        }

    }

    /**
     * Adds a vertex schema to the vertex schemas of a property graph.
     * @param vertexSchema  The vertex schema to add.
     */
    public void addVertexSchema(VertexSchema vertexSchema) {
        vertexSchemata.add(vertexSchema);
    }

    /**
     * Gets the vertex schemas of the property graph.
     * @return  A reference to the property graph's vertex schemas list.
     */
    public ArrayList<VertexSchema> getVertexSchemata() {
        return vertexSchemata;
    }

    /**
     * Adds an edge schema to the edge schemas of a property graph.
     * @param edgeSchema  The edge schema to add.
     */
    public void addEdgeSchema(EdgeSchema edgeSchema) {
        edgeSchemata.add(edgeSchema);
    }

    /**
     * Gets the edge schemas of the property graph.
     * @return A reference to the property graph's edge schemas list.
     */
    public ArrayList<EdgeSchema> getEdgeSchemata() {
        return edgeSchemata;
    }

    /**
     * Gets a set of the property names used in the schema of the property graph.
     * <p>The set is newly allocated and populated with each call.</p>
     * @return A set of strings containing the names of the properties used by the property graph.
     */
    public HashMap<String, Class<?>> getMapOfPropertyNamesToDataTypes() {

        HashMap<String, Class<?>> map = new HashMap<>();

        for (String property : propTypeMap.keySet()) {
            map.put(property, propTypeMap.get(property));
        }

        // nls todo : these aren't really helping... they shouldn't be doing anything, but in the legacy path,
        // they are being used


            for (EdgeSchema edgeSchema : edgeSchemata) {
                for (PropertySchema propertySchema : edgeSchema.getPropertySchemata()) {
                    if (!map.containsKey(propertySchema.getName())) {
                        try {
                            map.put(propertySchema.getName(), propertySchema.getType());
                        } catch (ClassNotFoundException e) {
                            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNKNOWN_CLASS_IN_GRAPHSCHEMA,
                                    "GRAPHBUILDER_ERROR: IO Exception during map-reduce job execution.", LOG, e);
                        }
                    }
                }
            }

            for (VertexSchema vertexSchema : vertexSchemata) {
                for (PropertySchema propertySchema : vertexSchema.getPropertySchemata()) {
                    if (!map.containsKey(propertySchema.getName())) {
                    try {
                        map.put(propertySchema.getName(), propertySchema.getType());
                    } catch (ClassNotFoundException e) {
                            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNKNOWN_CLASS_IN_GRAPHSCHEMA,
                                    "GRAPHBUILDER_ERROR: IO Exception during map-reduce job execution.", LOG, e);
                    }
                }
            }
        }

        return map;
    }

}
