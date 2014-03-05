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

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SchemaElement;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SchemaInferenceUtilsTest {

    final String THE_EDGE = "The Edge";
    final String BONO = "Bono";
    final String OTHERGUY = "One of the Other Guys";

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
    PropertySchema propertySchemaA_copy = new PropertySchema(A, dataTypeA);
    PropertySchema propertySchemaC = new PropertySchema(C, dataTypeC);
    PropertySchema propertySchemaD = new PropertySchema(D, dataTypeD);

    SchemaElement edgeSchema0 = SchemaElement.CreateEdgeSchemaElement(THE_EDGE);
    SchemaElement edgeSchema1 = SchemaElement.CreateEdgeSchemaElement(THE_EDGE);
    SchemaElement edgeSchema2 = SchemaElement.CreateEdgeSchemaElement(THE_EDGE);
    SchemaElement edgeSchema3 = SchemaElement.CreateEdgeSchemaElement(BONO);
    SchemaElement edgeSchema012 = SchemaElement.CreateEdgeSchemaElement(THE_EDGE);

    SchemaElement vertexSchema0 = SchemaElement.CreateVertexSchemaElement(null);
    SchemaElement vertexSchema1 = SchemaElement.CreateVertexSchemaElement(null);
    SchemaElement vertexSchema2 = SchemaElement.CreateVertexSchemaElement(OTHERGUY);
    SchemaElement vertexSchema3 = SchemaElement.CreateVertexSchemaElement(OTHERGUY);
    SchemaElement vertexSchema01 = SchemaElement.CreateVertexSchemaElement(null);
    SchemaElement vertexSchema23 = SchemaElement.CreateVertexSchemaElement(OTHERGUY);

    @Mock
    Mapper.Context mockedContext;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        // set up the edge schemata

        edgeSchema0.addPropertySchema(propertySchemaA);
        edgeSchema0.addPropertySchema(propertySchemaC);

        edgeSchema1.addPropertySchema(propertySchemaA);
        edgeSchema1.addPropertySchema(propertySchemaC);

        edgeSchema2.addPropertySchema(propertySchemaB);
        edgeSchema2.addPropertySchema(propertySchemaC);

        edgeSchema3.addPropertySchema(propertySchemaB);
        edgeSchema3.addPropertySchema(propertySchemaC);

        edgeSchema012.addPropertySchema(propertySchemaA);
        edgeSchema012.addPropertySchema(propertySchemaB);
        edgeSchema012.addPropertySchema(propertySchemaC);

        // set up the vertex schemata

        vertexSchema0.addPropertySchema(propertySchemaA);
        vertexSchema0.addPropertySchema(propertySchemaB);

        vertexSchema1.addPropertySchema(propertySchemaA_copy);
        vertexSchema1.addPropertySchema(propertySchemaC);

        vertexSchema2.addPropertySchema(propertySchemaA);
        vertexSchema2.addPropertySchema(propertySchemaC);

        vertexSchema3.addPropertySchema(propertySchemaA_copy);
        vertexSchema3.addPropertySchema(propertySchemaD);

        vertexSchema01.addPropertySchema(propertySchemaA);
        vertexSchema01.addPropertySchema(propertySchemaB);
        vertexSchema01.addPropertySchema(propertySchemaC);

        vertexSchema23.addPropertySchema(propertySchemaA_copy);
        vertexSchema23.addPropertySchema(propertySchemaD);
        vertexSchema23.addPropertySchema(propertySchemaC);
    }

    @Test
    public void testCombineSchemata() throws ClassNotFoundException {

        ArrayList<SchemaElement> inValues = new ArrayList<SchemaElement>();

        inValues.add(edgeSchema0);
        inValues.add(edgeSchema1);
        inValues.add(edgeSchema2);
        inValues.add(edgeSchema3);
        inValues.add(vertexSchema0);
        inValues.add(vertexSchema1);
        inValues.add(vertexSchema2);
        inValues.add(vertexSchema3);

        // now we set the expected out values

        HashSet<SchemaElement> outValues = new HashSet<SchemaElement>();

        outValues.add(vertexSchema23);
        outValues.add(vertexSchema01);
        outValues.add(edgeSchema012);
        outValues.add(edgeSchema3);

        MergeSchemataUtility mergeUtil = new MergeSchemataUtility();
        ArrayList<SchemaElement> testOut = mergeUtil.merge(inValues);

        // one copy each of THE_EDGE, BONO, propertyA and propertyD

        assertEquals(testOut.size(), 4);
        assertTrue(testOut.containsAll(outValues));
    }
}
