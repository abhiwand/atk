package com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema;

import org.junit.Test;

import static junit.framework.Assert.assertNotNull;


public class VertexSchemaTest {

    @Test
    public void testGetPropertySchemata() {
        VertexSchema vertexSchema = new VertexSchema();
        assertNotNull(vertexSchema.getPropertySchemata());
    }
}
