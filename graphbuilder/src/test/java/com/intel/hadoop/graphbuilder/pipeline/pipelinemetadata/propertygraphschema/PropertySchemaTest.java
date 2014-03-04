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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PropertySchemaTest {

    @Test
    public void testPropertySchemaConstructorGetters() throws ClassNotFoundException {

        final String A = "A";
        final Class<?> dataType = Integer.class;

        PropertySchema propertySchema = new PropertySchema(A, dataType);

        String testName = propertySchema.getName();

        Class<?> testDataType = propertySchema.getType();
        assertNotNull(testName);
        assertEquals("Should have been 0", 0, testName.compareTo(A));
        assertEquals(dataType, testDataType);
    }

    @Test
    public void testPropertySchemaSetGet() throws ClassNotFoundException {
        final String A = "A";
        final Class<?> dataTypeA = Integer.class;

        final String B = "A";
        final Class<?> dataTypeB = Float.class;

        final String C = "C";
        final Class<?> dataTypeC = String.class;

        PropertySchema propertySchemaA = new PropertySchema(A, dataTypeA);
        PropertySchema propertySchemaB = new PropertySchema(B, dataTypeB);

        propertySchemaA.setName(C);

        String testName1 = propertySchemaA.getName();
        Class<?> testType1 = propertySchemaA.getType();
        assertEquals("Should have been 0", 0, testName1.compareTo(C));
        assertEquals(dataTypeA, testType1);

        propertySchemaA.setType(dataTypeC);

        String testName2 = propertySchemaA.getName();
        Class<?> testType2 = propertySchemaA.getType();
        assertEquals("Should have been 0", 0, testName2.compareTo(C));
        assertEquals(dataTypeC, testType2);

        propertySchemaA.setName(A);

        String testName3 = propertySchemaA.getName();
        Class<?> testType3 = propertySchemaA.getType();
        assertEquals("Should have been 0", 0, testName3.compareTo(A));
        assertEquals(dataTypeC, testType3);

        propertySchemaA.setType(dataTypeA);

        String testName4 = propertySchemaA.getName();
        Class<?> testType4 = propertySchemaA.getType();
        assertEquals("Should have been 0", 0, testName4.compareTo(A));
        assertEquals(dataTypeA, testType4);

        String distinctName = propertySchemaB.getName();
        Class<?> distinctType = propertySchemaB.getType();
        assertEquals("Should have been 0", 0, distinctName.compareTo(B));
        assertEquals(dataTypeB, distinctType);
    }

    @Test
    public final void testWriteRead() throws IOException {
        final String A = "A";
        final Class<?> dataTypeA = Integer.class;

        final String B = "B";
        final Class<?> dataTypeB = Float.class;

        PropertySchema propertySchemaIn = new PropertySchema(A, dataTypeA);
        PropertySchema propertySchemaOut = new PropertySchema(B, dataTypeB);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream dataOutputStream = new DataOutputStream(baos);

        propertySchemaIn.write(dataOutputStream);
        dataOutputStream.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(bais);

        propertySchemaOut.readFields(dataInputStream);

        assertTrue(propertySchemaIn.getName().equals(propertySchemaOut.getName()));

        try {
            assertTrue(propertySchemaIn.getType().equals(propertySchemaOut.getType()));
        } catch (ClassNotFoundException e) {
            assertTrue(false);
        }
    }
}
