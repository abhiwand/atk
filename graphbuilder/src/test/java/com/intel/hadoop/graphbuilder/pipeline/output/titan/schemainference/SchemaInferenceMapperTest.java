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

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.VertexID;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SchemaElement;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SchemaInferenceMapperTest {

    static final StringType src = new StringType("src");
    static final VertexID<StringType> srcId = new VertexID<StringType>(src, null);
    static final StringType dst = new StringType("dst");
    static final VertexID<StringType> dstId = new VertexID<StringType>(dst, null);
    static final StringType label = new StringType("label");

    static final String key1 = "key";
    static final String key2 = "Ce n'est pas une clé";

    static final StringType value1 = new StringType("Outstanding Value");
    static final IntType value2 = new IntType(0);

    static final NullWritable key = NullWritable.get();
    static final Edge<StringType> edge = new Edge<StringType>(srcId, dstId, label);

    static final PropertySchema pSchema1 = new PropertySchema();
    static final PropertySchema pSchema2 = new PropertySchema();

    static final SchemaElement edgeSchema = SchemaElement.createEdgeSchemaElement(label.get());

    @Mock
    Mapper.Context mockedContext;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        edge.setProperty(key1, value1);
        edge.setProperty(key2, value2);

        pSchema1.setName(key1);
        pSchema1.setType(value1.getBaseType());
        pSchema2.setName(key2);
        pSchema2.setType(value2.getBaseType());

        edgeSchema.addPropertySchema(pSchema1);
        edgeSchema.addPropertySchema(pSchema2);
    }

    @Test
    public void testMap_edge() throws Exception {

        SerializedGraphElementStringTypeVids inValue = new SerializedGraphElementStringTypeVids();

        inValue.init(edge);

        SchemaInferenceMapper mapper = new SchemaInferenceMapper();

        mapper.map(key, inValue, mockedContext);

        SchemaElement edgeSchema = SchemaElement.createEdgeSchemaElement(label.get());

        edgeSchema.addPropertySchema(pSchema1);
        edgeSchema.addPropertySchema(pSchema2);

        ArgumentCaptor<SchemaElement> seopsCaptor =
                ArgumentCaptor.forClass(SchemaElement.class);

        ArgumentCaptor<Writable> keyCaptor =
                ArgumentCaptor.forClass(Writable.class);

        verify(mockedContext, times(1)).write(keyCaptor.capture(), seopsCaptor.capture());

        assertEquals(seopsCaptor.getValue(), edgeSchema);
        assertEquals(keyCaptor.getValue(), NullWritable.get());
    }

    @Test
    public void testWriteSchemata_Edge() throws Exception {

        SchemaInferenceMapper mapper = new SchemaInferenceMapper();

        mapper.writeSchemata(edgeSchema, mockedContext);

        Mockito.verify(mockedContext).write(NullWritable.get(), edgeSchema);
    }
}
