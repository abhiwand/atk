package com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema

import org.junit.Test

import static junit.framework.Assert.assertEquals
import static junit.framework.Assert.assertNotNull


class EdgeSchemaTest {

    @Test
    public void testEdgeSchemaConstructor() {

        final String THE_EDGE = "The Edge";
        final String BONO     = "Bono"

        EdgeSchema edgeSchema = new EdgeSchema(THE_EDGE);

        assertNotNull(edgeSchema.getPropertySchemata())
        assert(edgeSchema.getLabel().compareTo(THE_EDGE) == 0)
    }

    @Test
    public void testEdgeSchemaSetGetLabel() {

        final String THE_EDGE = "The Edge";
        final String BONO     = "Bono"

        EdgeSchema edgeSchema = new EdgeSchema(THE_EDGE);

        assert(edgeSchema.getLabel().compareTo(THE_EDGE) == 0)

        edgeSchema.setLabel(BONO)
        assert(edgeSchema.getLabel().compareTo(BONO) == 0)

        edgeSchema.setLabel(THE_EDGE)
        assert(edgeSchema.getLabel().compareTo(THE_EDGE) == 0)
    }

}
