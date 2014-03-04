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
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.graphelements.VertexID;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SerializedEdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
        VertexID<StringType> srcId = new VertexID<>(src, null);
        StringType dst = new StringType("dst");
        VertexID<StringType> dstId = new VertexID<>(dst, null);
        StringType label = new StringType("label");

        String key1 = "key";
        String key2 = "Ce n'est pas une clé";

        StringType value1 = new StringType("Outstanding Value");
        IntType value2 = new IntType(0);

        Edge<StringType> edge = new Edge<>(srcId, dstId, label);
        edge.setProperty(key1, value1);
        edge.setProperty(key2, value2);

        NullWritable key = NullWritable.get();
        SerializedGraphElementStringTypeVids value = new SerializedGraphElementStringTypeVids();
        value.init(edge);

        SchemaInferenceMapper mapper = new SchemaInferenceMapper();

        mapper.map(key, value, mockedContext);

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
        SerializedEdgeOrPropertySchema serializedPSchema1 = new SerializedEdgeOrPropertySchema(pSchema1);
        SerializedEdgeOrPropertySchema serializedPSchema2 = new SerializedEdgeOrPropertySchema(pSchema2);

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

    @Test
    public void testWriteSchemata() throws Exception {

        // let's write a property schema
        final String A = "A";
        final Class<?> dataType = Integer.class;

        PropertySchema propertySchema = new PropertySchema(A, dataType);
        ArrayList<EdgeOrPropertySchema> list = new ArrayList<>();
        list.add(propertySchema);

        SchemaInferenceMapper mapper = new SchemaInferenceMapper();

        mapper.writeSchemata(list, mockedContext);

        Mockito.verify(mockedContext).write(NullWritable.get(), new SerializedEdgeOrPropertySchema(propertySchema));

        // now lets write an edge schema

        final String THE_EDGE = "The Edge";

        EdgeSchema edgeSchema = new EdgeSchema(THE_EDGE);
        edgeSchema.addPropertySchema(propertySchema);

        list.clear();
        list.add(edgeSchema);

        mapper.writeSchemata(list, mockedContext);

        Mockito.verify(mockedContext).write(NullWritable.get(), new SerializedEdgeOrPropertySchema(edgeSchema));
    }

    @Test
    public void testGetSchemaInfo() throws Exception {

        PropertyMap map0 = new PropertyMap();
        map0.setProperty("name", new StringType("Alice"));
        map0.setProperty("age", new LongType(36));
        map0.setProperty("dept", new StringType("IntelCorp"));

        PropertyMap mapV = new PropertyMap();
        mapV.setProperty("name", new StringType("Bob"));
        mapV.setProperty("weight", new IntType(232));
        mapV.setProperty("dept", new StringType("IntelLabs"));

        Edge<StringType> edge0 = new Edge<>(
                new StringType("Employee001"), new StringType("Employee002"),
                new StringType("isConnected"),
                map0);

        SerializedGraphElementStringTypeVids sEdge = new SerializedGraphElementStringTypeVids();
        sEdge.init(edge0);

        StringType vertexName = new StringType("The Greatest Vertex EVER");
        StringType vertexLabel = new StringType("label");

        Vertex<StringType> vertex = new Vertex<>(vertexName, vertexLabel, mapV);

        SerializedGraphElementStringTypeVids sVertex = new SerializedGraphElementStringTypeVids();
        sVertex.init(vertex);

        SchemaInferenceMapper mapper = new SchemaInferenceMapper();

        ArrayList<EdgeOrPropertySchema> vSchemata = mapper.schemataFromGraphElement(sVertex);

        assertEquals(vSchemata.size(), 3);
        for (EdgeOrPropertySchema schema : vSchemata) {

            if (schema instanceof PropertySchema) {
                PropertySchema pSchema = (PropertySchema) schema;
                String name = pSchema.getName();
                Class<?> klass = pSchema.getType();

                assertTrue((name.equals("name") && klass.equals(String.class))
                        || (name.equals("weight") && klass.equals(Integer.class))
                        || (name.equals("dept") && klass.equals(String.class)));
            } else {
                fail("Non- PropertySchema in alleged list of PropertySchema generted from vertex.");
            }
        }

        ArrayList<EdgeOrPropertySchema> eSchemata = mapper.schemataFromGraphElement(sEdge);
        map0.setProperty("name", new StringType("Alice"));
        map0.setProperty("age", new LongType(36));
        map0.setProperty("dept", new StringType("IntelCorp"));

        assertEquals(eSchemata.size(), 4);

        for (EdgeOrPropertySchema schema : eSchemata) {

            if (schema instanceof PropertySchema) {
                PropertySchema pSchema = (PropertySchema) schema;
                String name = pSchema.getName();
                Class<?> klass = pSchema.getType();

                assertTrue((name.equals("name") && klass == String.class)
                        || (name.equals("age") && klass == Long.class)
                        || (name.equals("dept") && klass == String.class));
            } else if (schema instanceof EdgeSchema) {
                EdgeSchema eSchema = (EdgeSchema) schema;

                String label = eSchema.getLabel();
                assertEquals(label, "isConnected");

                HashSet<PropertySchema> pSchemata = eSchema.getPropertySchemata();

                assertEquals(pSchemata.size(), 3);

                for (EdgeOrPropertySchema nestedSchema : pSchemata) {

                    if (nestedSchema instanceof PropertySchema) {
                        PropertySchema pSchema = (PropertySchema) nestedSchema;
                        String name = pSchema.getName();
                        Class<?> klass = pSchema.getType();

                        assertTrue((name.equals("name") && klass.equals(String.class))
                                || (name.equals("age") && klass.equals(Long.class))
                                || (name.equals("dept") && klass.equals(String.class)));
                    } else {
                        fail("Edge schema contains a non-propertySchema object in its lists of PropertySchema");
                    }
                }
            } else {
                fail("Non EdgeOrPropertySchema object returned in alleged schema list.");
            }
        }
    }
}
