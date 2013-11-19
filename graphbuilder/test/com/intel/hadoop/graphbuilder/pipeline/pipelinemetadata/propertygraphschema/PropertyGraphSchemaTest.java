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
