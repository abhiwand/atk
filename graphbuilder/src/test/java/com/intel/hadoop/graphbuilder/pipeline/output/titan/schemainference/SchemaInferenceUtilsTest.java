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

package com.intel.hadoop.graphbuilder.pipeline.output.titan.schemainference;

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SerializedEdgeOrPropertySchema;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.*;

public class SchemaInferenceUtilsTest {

    @Mock
    Mapper.Context mockedContext;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCombineSchemata() {

        final String A = "A";
        final Class<?> dataTypeA = Integer.class;

        final String B = "B";
        final Class<?> dataTypeB = Float.class;

        final String C = "C";
        final Class<?> dataTypeC = String.class;

        final String D = "D";
        final Class<?> dataTypeD = Long.class;

        PropertySchema propertySchemaA = new PropertySchema(A, dataTypeA);
        PropertySchema propertySchemaB = new PropertySchema(B, dataTypeB);
        PropertySchema propertySchemaA2 = new PropertySchema(A, dataTypeA);
        PropertySchema propertySchemaC = new PropertySchema(C, dataTypeC);
        PropertySchema propertySchemaD = new PropertySchema(D, dataTypeD);

        // add the property schema

        ArrayList<SerializedEdgeOrPropertySchema> values = new ArrayList<>();
        values.add(new SerializedEdgeOrPropertySchema(propertySchemaA));
        values.add(new SerializedEdgeOrPropertySchema(propertySchemaA2));
        values.add(new SerializedEdgeOrPropertySchema(propertySchemaD));

        // now combine some edge schema

        final String THE_EDGE = "The Edge";
        final String BONO = "Bono";

        EdgeSchema edgeSchema0 = new EdgeSchema(THE_EDGE);
        edgeSchema0.addPropertySchema(propertySchemaA);
        edgeSchema0.addPropertySchema(propertySchemaC);

        EdgeSchema edgeSchema1 = new EdgeSchema(THE_EDGE);
        edgeSchema1.addPropertySchema(propertySchemaA);
        edgeSchema1.addPropertySchema(propertySchemaC);

        EdgeSchema edgeSchema2 = new EdgeSchema(THE_EDGE);
        edgeSchema2.addPropertySchema(propertySchemaB);
        edgeSchema2.addPropertySchema(propertySchemaC);

        EdgeSchema edgeSchema3 = new EdgeSchema(BONO);
        edgeSchema3.addPropertySchema(propertySchemaB);
        edgeSchema3.addPropertySchema(propertySchemaC);

        values.add(new SerializedEdgeOrPropertySchema(edgeSchema0));
        values.add(new SerializedEdgeOrPropertySchema(edgeSchema1));
        values.add(new SerializedEdgeOrPropertySchema(edgeSchema2));
        values.add(new SerializedEdgeOrPropertySchema(edgeSchema3));

        Logger LOG = Logger.getLogger
                (MergeSchemataUtility.class);

        ArrayList<EdgeOrPropertySchema> testOut = MergeSchemataUtility.merge(values, LOG);

        // one copy each of THE_EDGE, BONO, propertyA and propertyD

        assertEquals(testOut.size(), 4);

        for (EdgeOrPropertySchema schema : testOut)
            if (schema instanceof PropertySchema) {
                PropertySchema propertySchema = (PropertySchema) schema;
                String name = propertySchema.getName();
                try {
                    Class<?> type = propertySchema.getType();
                    assertTrue((name.equals(A) && type.equals(dataTypeA)
                            || (propertySchema.getName().equals(D) && type.equals(dataTypeD))));
                } catch (ClassNotFoundException e) {
                    fail("Class not found exception.");
                }
            } else {
                EdgeSchema edgeSchema = (EdgeSchema) schema;
                String label = edgeSchema.getLabel();

                assertTrue(label.equals(BONO) || label.equals(THE_EDGE));

                HashSet<PropertySchema> propertySchemata = edgeSchema.getPropertySchemata();

                if (label.equals(THE_EDGE)) {
                    assert (propertySchemata.size() == 3);

                    for (PropertySchema pSchema : propertySchemata) {
                        try {
                            assertTrue((pSchema.getName().equals(A) && pSchema.getType().equals(dataTypeA))
                                    || (pSchema.getName().equals(B) && pSchema.getType().equals(dataTypeB))
                                    || (pSchema.getName().equals(C) && pSchema.getType().equals(dataTypeC)));
                        } catch (Exception e) {
                            fail("Class not found exception.");
                        }
                    }
                } else {
                    assert (propertySchemata.size() == 2);

                    for (PropertySchema pSchema : propertySchemata) {
                        try {
                            assertTrue((pSchema.getName().equals(B) && pSchema.getType().equals(dataTypeB))
                                    || (pSchema.getName().equals(C) && pSchema.getType().equals(dataTypeC)));
                        } catch (Exception e) {
                            fail("Class not found exception.");
                        }
                    }
                }
            }
    }
}
