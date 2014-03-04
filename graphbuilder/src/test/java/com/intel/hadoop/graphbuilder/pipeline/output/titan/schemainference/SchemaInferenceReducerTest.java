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

import com.intel.hadoop.graphbuilder.pipeline.output.titan.GBTitanKey;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.KeyCommandLineParser;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanGraphInitializer;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SerializedEdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({GraphDatabaseConnector.class})
public class SchemaInferenceReducerTest {

    @Mock
    TitanGraphInitializer mockedInitializer;

    @Mock
    Reducer.Context mockedContext;

    @Mock
    GraphDatabaseConnector mockedGraphDatabaseConnector;

    @Mock
    TitanGraph mockedTitanGraph;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testReduce() throws Exception {

        String keyCommandLine = "";

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

        inValues.add(new SerializedEdgeOrPropertySchema(edgeSchema0));
        inValues.add(new SerializedEdgeOrPropertySchema(edgeSchema1));
        inValues.add(new SerializedEdgeOrPropertySchema(edgeSchema2));
        inValues.add(new SerializedEdgeOrPropertySchema(edgeSchema3));

        // now we set up the values expected to be passed to the initializer
        EdgeSchema edgeSchema012 = new EdgeSchema(THE_EDGE);
        edgeSchema012.addPropertySchema(propertySchemaA);
        edgeSchema012.addPropertySchema(propertySchemaB);
        edgeSchema012.addPropertySchema(propertySchemaC);

        PropertyGraphSchema propertyGraphSchema = new PropertyGraphSchema();

        propertyGraphSchema.addPropertySchema(propertySchemaA);
        propertyGraphSchema.addPropertySchema(propertySchemaD);
        propertyGraphSchema.addEdgeSchema(edgeSchema012);
        propertyGraphSchema.addEdgeSchema(edgeSchema3);

        // now those declared keys... oh joy
        List<GBTitanKey> declaredKeys = new KeyCommandLineParser().parse(keyCommandLine);

        SchemaInferenceReducer reducer = new SchemaInferenceReducer();
        Configuration conf = new Configuration();
        conf.set("special key-value pair,", "so you can't accept a default value by mistake");

        reducer.setInitializer(mockedInitializer);
        when(mockedContext.getConfiguration()).thenReturn(conf);

        reducer.reduce(NullWritable.get(), inValues, mockedContext);

        Mockito.verify(mockedInitializer).setConf(conf);
        Mockito.verify(mockedInitializer).setGraphSchema(propertyGraphSchema);
        Mockito.verify(mockedInitializer).setDeclaredKeys(declaredKeys);
    }
}
