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

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import static org.junit.Assert.*;

public class PropertyGraphSchemaTest {

    @Test
    public void testPropertyGraphSchemaConstructor() {

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        assertNotNull(graphSchema.getPropertySchemata());
        assertNotNull(graphSchema.getEdgeSchemata());

        assertNotSame(graphSchema.getEdgeSchemata(), graphSchema.getPropertySchemata());
    }

    @Test
    public void testAddEdgeSchema() {

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        EdgeSchema edgeSchema = new EdgeSchema("foo");

        graphSchema.addEdgeSchema(edgeSchema);

        assertTrue(graphSchema.getEdgeSchemata().contains(edgeSchema));
    }

    @Test
    public void testGetMapOfPropertyNamesToDataTypes() {

        final String PLANET_OF_THE_STRINGS = "planet of the strings";
        final String PLANET_OF_THE_FLOATS = "planet of the floags";

        PropertySchema planetOfStrings = new PropertySchema(PLANET_OF_THE_STRINGS, String.class);
        PropertySchema planetOfFloats = new PropertySchema(PLANET_OF_THE_FLOATS, Float.class);

        EdgeSchema edgeSchemaZ = new EdgeSchema("dr zaius");

        edgeSchemaZ.setLabel("you d--- dirty ape");
        edgeSchemaZ.addPropertySchema(planetOfStrings);

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        graphSchema.addPropertySchema(planetOfFloats);
        graphSchema.addEdgeSchema(edgeSchemaZ);

        Set<PropertySchema> outSchema = graphSchema.getPropertySchemata();

        assertTrue(outSchema.contains(planetOfFloats));
        assertTrue(outSchema.contains(planetOfStrings));
        assertEquals(outSchema.size(), 2);
    }

    @Test
    public void testEqualsHashCode() {
        final String PLANET_OF_THE_STRINGS = "planet of the strings";
        final String PLANET_OF_THE_FLOATS = "planet of the floats";

        PropertySchema planetOfStrings = new PropertySchema(PLANET_OF_THE_STRINGS, String.class);
        PropertySchema planetOfFloats = new PropertySchema(PLANET_OF_THE_FLOATS, Float.class);

        EdgeSchema edgeSchemaZ = new EdgeSchema("dr zaius");

        edgeSchemaZ.setLabel("you d--- dirty ape");
        edgeSchemaZ.addPropertySchema(planetOfStrings);

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();
        graphSchema.addPropertySchema(planetOfFloats);
        graphSchema.addEdgeSchema(edgeSchemaZ);

        // now a copy of everything
        final String PLANET_OF_THE_FLOATS2 = "planet of the floats";

        PropertySchema planetOfFloats2 = new PropertySchema(PLANET_OF_THE_FLOATS2, Float.class);

        EdgeSchema edgeSchemaZ2 = new EdgeSchema("dr zaius");

        edgeSchemaZ2.setLabel("you d--- dirty ape");
        edgeSchemaZ2.addPropertySchema(planetOfStrings);

        PropertyGraphSchema graphSchema2 = new PropertyGraphSchema();
        graphSchema2.addPropertySchema(planetOfFloats2);
        graphSchema2.addEdgeSchema(edgeSchemaZ2);

        assertEquals(graphSchema, graphSchema2);
        assertEquals(graphSchema.hashCode(), graphSchema2.hashCode());
    }

    @Test
    public void testTwoDifferentConstructors() {
        final String PLANET_OF_THE_STRINGS = "planet of the strings";
        final String PLANET_OF_THE_FLOATS = "planet of the floats";

        final String DR_ZAIUS = "Dr. Zaius";
        final String D_DIRTY_APE = "you d--- dirty ape";

        PropertySchema planetOfStrings = new PropertySchema(PLANET_OF_THE_STRINGS, String.class);
        PropertySchema planetOfFloats = new PropertySchema(PLANET_OF_THE_FLOATS, Float.class);

        EdgeSchema edgeSchemaZ = new EdgeSchema(DR_ZAIUS);

        edgeSchemaZ.setLabel(D_DIRTY_APE);
        edgeSchemaZ.addPropertySchema(planetOfStrings);

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();
        graphSchema.addPropertySchema(planetOfFloats);
        graphSchema.addEdgeSchema(edgeSchemaZ);

        HashMap<String, Class<?>> propTypeMap = new HashMap<>();
        HashMap<String, ArrayList<String>> edgeSignatures = new HashMap<>();
        propTypeMap.put(PLANET_OF_THE_FLOATS, Float.class);
        propTypeMap.put(PLANET_OF_THE_STRINGS, String.class);

        ArrayList<String> signature = new ArrayList<>();
        signature.add(PLANET_OF_THE_STRINGS);

        edgeSignatures.put(D_DIRTY_APE, signature);

        PropertyGraphSchema graphSchema2 = new PropertyGraphSchema(propTypeMap, edgeSignatures);

        assertEquals(graphSchema, graphSchema2);
    }
}
