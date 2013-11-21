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

package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNotSame;

public class PropertyGraphSchemaTest {

    @Test
    public void testPropertyGraphSchemaConstructor() {

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        assertNotNull(graphSchema.getVertexSchemata());
        assertNotNull(graphSchema.getEdgeSchemata());

        assertNotSame(graphSchema.getEdgeSchemata(), graphSchema.getVertexSchemata());
    }

    @Test
    public void testAddVertexSchema() {

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        VertexSchema vertexSchema = new VertexSchema();

        graphSchema.addVertexSchema(vertexSchema);

        assert(graphSchema.getVertexSchemata().contains(vertexSchema));
    }

    @Test
    public void testAddEdgeSchema() {

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        EdgeSchema edgeSchema = new EdgeSchema("foo");

        graphSchema.addEdgeSchema(edgeSchema);

        assert(graphSchema.getEdgeSchemata().contains(edgeSchema));
    }
}
