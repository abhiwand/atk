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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SchemaInferenceCombinerTest {

    @Mock
    Reducer.Context mockedContext;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testReduce() throws Exception {

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

        // add the roperty schema

        ArrayList<SerializedEdgeOrPropertySchema> inValues = new ArrayList<>();
        inValues.add(new SerializedEdgeOrPropertySchema(propertySchemaA));
        inValues.add(new SerializedEdgeOrPropertySchema(propertySchemaA2));
        inValues.add(new SerializedEdgeOrPropertySchema(propertySchemaD));

        // now combine some edge schema

        final String THE_EDGE = "The Edge";
        final String BONO  = "Bono";

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



        inValues.add(new SerializedEdgeOrPropertySchema(edgeSchema0));
        inValues.add(new SerializedEdgeOrPropertySchema(edgeSchema1));
        inValues.add(new SerializedEdgeOrPropertySchema(edgeSchema2));
        inValues.add(new SerializedEdgeOrPropertySchema(edgeSchema3));

        // now we set the expected out values

        EdgeSchema edgeSchema012 = new EdgeSchema(THE_EDGE);
        edgeSchema012.addPropertySchema(propertySchemaA);
        edgeSchema012.addPropertySchema(propertySchemaB);
        edgeSchema012.addPropertySchema(propertySchemaC);

        HashSet<SerializedEdgeOrPropertySchema> outValues = new HashSet<>();

        outValues.add(new SerializedEdgeOrPropertySchema(propertySchemaA));
        outValues.add(new SerializedEdgeOrPropertySchema(propertySchemaD));
        outValues.add(new SerializedEdgeOrPropertySchema(edgeSchema012));
        outValues.add(new SerializedEdgeOrPropertySchema(edgeSchema3));


        Logger LOG =  Logger.getLogger
                (SchemaInferenceUtils.class);

        SchemaInferenceCombiner combiner = new SchemaInferenceCombiner();

        ArgumentCaptor<SerializedEdgeOrPropertySchema> seopsCaptor =
                ArgumentCaptor.forClass(SerializedEdgeOrPropertySchema.class);

        ArgumentCaptor<Writable> keyCaptor =
                ArgumentCaptor.forClass(Writable.class);


        combiner.reduce(NullWritable.get(), inValues, mockedContext);

        verify(mockedContext, times(4)).write(keyCaptor.capture(), seopsCaptor.capture());

        List<SerializedEdgeOrPropertySchema> capturedSeops = seopsCaptor.getAllValues();
        List<Writable> capturedKeys = keyCaptor.getAllValues();

        for (SerializedEdgeOrPropertySchema schema : capturedSeops) {

            if (schema == null) {
                fail("null graph shema returned from mapper");
            } else {
                assertTrue(outValues.contains(schema));
            }
        }

        for (Writable k : capturedKeys) {
            assertEquals(k, NullWritable.get());
        }
    }
}
