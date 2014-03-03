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

import com.intel.hadoop.graphbuilder.pipeline.output.titan.schemainference.*;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.VertexID;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SerializedEdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class SchemaInferenceMapperTest {

    @Mock
    Mapper.Context mockedContext;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testMap() throws Exception {

        StringType src = new StringType("src");
        VertexID<StringType> srcId = new VertexID<StringType>(src, null);
        StringType dst = new StringType("dst");
        VertexID<StringType> dstId = new VertexID<StringType>(dst,null);
        StringType label = new StringType("label");

        String key1 = new String("key");
        String key2 = new String("Ce n'est pas une clé");

        StringType value1 = new StringType("Outstanding Value");
        IntType value2 = new IntType(0);

        Edge<StringType> edge = new Edge<StringType>(srcId, dstId, label);
        edge.setProperty(key1, value1);
        edge.setProperty(key2, value2);


        NullWritable key  =  NullWritable.get();
        SerializedGraphElementStringTypeVids value = new SerializedGraphElementStringTypeVids();
        value.init(edge);

        SchemaInferenceMapper mapper = new SchemaInferenceMapper();

        mapper.map(key,value,mockedContext);

        PropertySchema pSchema1 = new PropertySchema();
        pSchema1.setName(key1);
        pSchema1.setType(value1.getBaseType());

        PropertySchema pSchema2 = new PropertySchema();
        pSchema2.setName(key2);
        pSchema2.setType(value2.getBaseType());

        EdgeSchema edgeSchema = new EdgeSchema(label.get());

        edgeSchema.addPropertySchema(pSchema1);
        edgeSchema.addPropertySchema(pSchema2);

        ArgumentCaptor<SerializedEdgeOrPropertySchema> seopsCaptor =
                ArgumentCaptor.forClass(SerializedEdgeOrPropertySchema.class);

        ArgumentCaptor<Writable> keyCaptor =
                ArgumentCaptor.forClass(Writable.class);


        verify(mockedContext, times(3)).write(keyCaptor.capture(), seopsCaptor.capture());

        List<SerializedEdgeOrPropertySchema> capturedSeops = seopsCaptor.getAllValues();
        List<Writable> capturedKeys = keyCaptor.getAllValues();

        SerializedEdgeOrPropertySchema serializedEdgeSchema = new SerializedEdgeOrPropertySchema(edgeSchema);
        SerializedEdgeOrPropertySchema serializedPSchema1   = new SerializedEdgeOrPropertySchema(pSchema1);
        SerializedEdgeOrPropertySchema serializedPSchema2   = new SerializedEdgeOrPropertySchema(pSchema2);

        for (SerializedEdgeOrPropertySchema schema : capturedSeops) {

            if (schema == null) {
                fail("null graph shema returned from mapper");
            } else {
                assertTrue(schema.equals(serializedEdgeSchema)
                        || schema.equals(serializedPSchema1)
                        || schema.equals(serializedPSchema2));
            }
        }

        for (Writable k : capturedKeys) {
            assertEquals(k, NullWritable.get());
        }
    }
}
