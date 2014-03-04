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

import java.io.*;

import static junit.framework.Assert.assertSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SerializedEdgeOrPropertySchemaTest {

    final String PLANET_OF_THE_STRINGS = "planet of the strings";
    final String PLANET_OF_THE_FLOATS = "planet of the floats";

    PropertySchema planetOfStrings = new PropertySchema(PLANET_OF_THE_STRINGS, String.class);
    PropertySchema planetOfFloats = new PropertySchema(PLANET_OF_THE_FLOATS, Float.class);

    final String DR_ZAIUS = "Dr. Zaius";
    final String D_DIRTY_APE = "you d--- dirty ape";

    @Test
    public void testBasicEdgeSchema() throws Exception {

        EdgeSchema edgeSchemaZ = new EdgeSchema(DR_ZAIUS);
        edgeSchemaZ.addPropertySchema(planetOfStrings);

        EdgeSchema edgeSchemaD = new EdgeSchema(D_DIRTY_APE);
        edgeSchemaD.addPropertySchema(planetOfFloats);

        SerializedEdgeOrPropertySchema schema = new SerializedEdgeOrPropertySchema();
        schema.setSchema(edgeSchemaZ);

        assertSame(edgeSchemaZ, schema.getSchema());

        schema.setSchema(edgeSchemaD);
        assertSame(edgeSchemaD, schema.getSchema());
    }

    @Test
    public final void testWriteRead() throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream dataOutputStream = new DataOutputStream(baos);

        SerializedEdgeOrPropertySchema inSerializedSchema = new SerializedEdgeOrPropertySchema();
        inSerializedSchema.setSchema(planetOfStrings);

        SerializedEdgeOrPropertySchema outSerializedSchema = new SerializedEdgeOrPropertySchema();
        outSerializedSchema.setSchema(planetOfFloats);

        inSerializedSchema.write(dataOutputStream);
        dataOutputStream.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(bais);

        outSerializedSchema.readFields(dataInputStream);

        assertTrue(inSerializedSchema.getSchema().equals(outSerializedSchema.getSchema()));
    }

    @Test
    public void testEqualsHashCode() throws Exception {

        EdgeSchema one = new EdgeSchema("edge");
        EdgeSchema two = new EdgeSchema("edge");

        SerializedEdgeOrPropertySchema serializedOne = new SerializedEdgeOrPropertySchema(one);
        SerializedEdgeOrPropertySchema serializedTwo = new SerializedEdgeOrPropertySchema(two);

        assertEquals(serializedOne, serializedTwo);
        assertEquals(serializedOne.hashCode(), serializedTwo.hashCode());
    }
}
