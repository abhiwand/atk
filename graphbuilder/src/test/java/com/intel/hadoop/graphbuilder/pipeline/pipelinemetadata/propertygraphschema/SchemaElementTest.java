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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SchemaElementTest {

    final String PLANET_OF_THE_STRINGS = "planet of the strings";
    final String PLANET_OF_THE_FLOATS = "planet of the floats";

    PropertySchema planetOfStrings = new PropertySchema(PLANET_OF_THE_STRINGS, String.class);
    PropertySchema planetOfFloats = new PropertySchema(PLANET_OF_THE_FLOATS, Float.class);

    final String DR_ZAIUS = "Dr. Zaius";
    final String D_DIRTY_APE = "you d--- dirty ape";

    @Test
    public final void test_write_read() throws IOException {

        SchemaElement inSchema = SchemaElement.createEdgeSchemaElement(DR_ZAIUS);
        inSchema.addPropertySchema(planetOfStrings);

        SchemaElement outSchema = SchemaElement.createVertexSchemaElement(D_DIRTY_APE);
        outSchema.addPropertySchema(planetOfFloats);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream dataOutputStream = new DataOutputStream(baos);

        inSchema.write(dataOutputStream);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(bais);
        dataOutputStream.flush();
        outSchema.readFields(dataInputStream);

        assertTrue(inSchema.equals(outSchema));
    }

    @Test
    public void test_consistency_of_equals_and_HashCode() throws Exception {

        SchemaElement serializedOne = SchemaElement.createVertexSchemaElement(D_DIRTY_APE);
        SchemaElement serializedTwo = SchemaElement.createVertexSchemaElement(D_DIRTY_APE);

        assertEquals(serializedOne, serializedTwo);
        assertEquals(serializedOne.hashCode(), serializedTwo.hashCode());
    }
}
