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

/**
 * The schema or "signature" of a property graph. It contains all the possible types of edges and vertices that it may
 * contain. (Possibly it contains types for edges or vertices that are not witnessed by any element present in the
 * graph.)
 *
 * The expected use of this information is declaring keys for loading the constructed graph into a graph database.
 */
public class PropertyGraphSchema {

    private ArrayList<VertexSchema> vertexSchemata;
    private ArrayList<EdgeSchema>   edgeSchemata;

    public PropertyGraphSchema() {
        vertexSchemata = new ArrayList<VertexSchema>();
        edgeSchemata   = new ArrayList<EdgeSchema>();
    }

    public void addVertexSchema(VertexSchema vertexSchema) {
        vertexSchemata.add(vertexSchema);
    }

    public ArrayList<VertexSchema> getVertexSchemata() {
        return vertexSchemata;
    }

    public void addEdgeSchema(EdgeSchema edgeSchema) {
        edgeSchemata.add(edgeSchema);
    }

    public ArrayList<EdgeSchema> getEdgeSchemata() {
        return edgeSchemata;
    }

}
