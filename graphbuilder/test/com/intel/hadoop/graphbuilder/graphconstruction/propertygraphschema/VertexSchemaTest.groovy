package com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema

import org.junit.Test

import static junit.framework.Assert.assertEquals
import static junit.framework.Assert.assertNotNull


class VertexSchemaTest {

    @Test
    void testGetPropertySchemata() {
        VertexSchema vertexSchema = new VertexSchema();
        assertNotNull(vertexSchema.getPropertySchemata());
    }
}
