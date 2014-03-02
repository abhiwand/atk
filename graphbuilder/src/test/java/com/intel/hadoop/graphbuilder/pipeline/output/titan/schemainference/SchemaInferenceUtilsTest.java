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
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeOrPropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashSet;

public class SchemaInferenceUtilsTest {

    @Mock
    Mapper.Context mockedContext;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testWriteSchemata() throws Exception {

        // let's write a property schema
        final String A = "A";
        final Class<?> dataType = Integer.class;

        PropertySchema propertySchema = new PropertySchema(A, dataType);
        ArrayList<EdgeOrPropertySchema> list = new ArrayList<>();
        list.add(propertySchema);

        SchemaInferenceUtils.writeSchemata(list, mockedContext);

        Mockito.verify(mockedContext).write(NullWritable.get(), propertySchema);


        // now lets write an edge schema

        final String THE_EDGE = "The Edge";

        EdgeSchema edgeSchema = new EdgeSchema(THE_EDGE);
        edgeSchema.addPropertySchema(propertySchema);

        list.clear();
        list.add(edgeSchema);

        SchemaInferenceUtils.writeSchemata(list, mockedContext);

        Mockito.verify(mockedContext).write(NullWritable.get(), edgeSchema);

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

        ArrayList<EdgeOrPropertySchema> vSchemata = SchemaInferenceUtils.getSchemaInfo(sVertex);

        assert (vSchemata.size() == 3);
        for (EdgeOrPropertySchema schema : vSchemata) {

            if (schema instanceof PropertySchema) {
                PropertySchema pSchema = (PropertySchema) schema;
                String name = pSchema.getName();
                Class<?> klass = pSchema.getType();

                assert ((name.equals("name") && klass.equals(String.class))
                        || (name.equals("weight") && klass.equals(Integer.class))
                        || (name.equals("dept") && klass.equals(String.class)));
            } else {
                assert (false);
            }
        }

        ArrayList<EdgeOrPropertySchema> eSchemata = SchemaInferenceUtils.getSchemaInfo(sEdge);
        map0.setProperty("name", new StringType("Alice"));
        map0.setProperty("age", new LongType(36));
        map0.setProperty("dept", new StringType("IntelCorp"));

        assert (eSchemata.size() == 4);
        for (EdgeOrPropertySchema schema : eSchemata) {

            if (schema instanceof PropertySchema) {
                PropertySchema pSchema = (PropertySchema) schema;
                String name = pSchema.getName();
                Class<?> klass = pSchema.getType();

                assert ((name.equals("name") && klass == String.class)
                        || (name.equals("age") && klass == Long.class)
                        || (name.equals("dept") && klass == String.class));
            } else if (schema instanceof EdgeSchema) {
                EdgeSchema eSchema = (EdgeSchema) schema;

                String label = eSchema.getLabel();
                assert (label.equals("isConnected"));

                HashSet<PropertySchema> pSchemata = eSchema.getPropertySchemata();

                assert (pSchemata.size() == 3);
                for (EdgeOrPropertySchema nestedSchema : pSchemata) {

                    if (nestedSchema instanceof PropertySchema) {
                        PropertySchema pSchema = (PropertySchema) nestedSchema;
                        String name = pSchema.getName();
                        Class<?> klass = pSchema.getType();

                        assert ((name.equals("name") && klass.equals(String.class))
                                || (name.equals("age") && klass.equals(Long.class))
                                || (name.equals("dept") && klass.equals(String.class)));
                    } else {
                        assert (false);
                    }
                }
            } else {
                assert (false);
            }
        }
    }

    @Test
    public void testCombineSchemata() {

        // EdgeOrPropertySchema combineSchemata(Iterable<EdgeOrPropertySchema> values)

        // combine two edge schemata, and two property schemata... simple as that... get them from those tests
        final String A = "A";
        final Class<?> dataTypeA = Integer.class;

        final String B = "B";
        final Class<?> dataTypeB = Float.class;

        final String C = "C";
        final Class<?> dataTypeC = String.class;

        PropertySchema propertySchemaA = new PropertySchema(A, dataTypeA);
        PropertySchema propertySchemaB = new PropertySchema(B, dataTypeB);
        PropertySchema propertySchemaA2 = new PropertySchema(A, dataTypeA);
        PropertySchema propertySchemaC = new PropertySchema(C, dataTypeC);

        // test combination of property schema

        ArrayList<EdgeOrPropertySchema> values = new ArrayList<>();
        values.add(propertySchemaA);
        values.add(propertySchemaA2);

        ArrayList<EdgeOrPropertySchema> propSchemaTestOut = SchemaInferenceUtils.combineSchemata(values);

        // nls todo
        // redo assertions
        /*
        assert (propSchemaTestOut instanceof ArrayPropertySchema);

        PropertySchema propertySchema = (PropertySchema) propSchemaTestOut;
        assert (propertySchema.getName().equals(A));
        try {
            assert (propertySchema.getType().equals(dataTypeA));
        } catch (Exception e) {
            assert (false);
        }
        */

        // now combine some edge schema

        final String THE_EDGE = "The Edge";

        EdgeSchema edgeSchema1 = new EdgeSchema(THE_EDGE);
        edgeSchema1.addPropertySchema(propertySchemaA);
        edgeSchema1.addPropertySchema(propertySchemaC);

        EdgeSchema edgeSchema2 = new EdgeSchema(THE_EDGE);
        edgeSchema2.addPropertySchema(propertySchemaB);
        edgeSchema2.addPropertySchema(propertySchemaC);

        values.clear();
        values.add(edgeSchema1);
        values.add(edgeSchema2);

        // nls todo
        /*
        EdgeOrPropertySchema edgeSchemaTestOut = SchemaInferenceUtils.combineSchemata(values);

        assert (edgeSchemaTestOut instanceof EdgeSchema);

        EdgeSchema edgeSchema = (EdgeSchema) edgeSchemaTestOut;
        assert (edgeSchema.getLabel().equals(THE_EDGE));

        HashSet<PropertySchema> propertySchemata = edgeSchema.getPropertySchemata();

        assert (propertySchemata.size() == 3);

        for (PropertySchema pSchema : propertySchemata) {
            try {
                assert ((pSchema.getName().equals(A) && pSchema.getType().equals(dataTypeA))
                        || (pSchema.getName().equals(B) && pSchema.getType().equals(dataTypeB))
                        || (pSchema.getName().equals(C) && pSchema.getType().equals(dataTypeC)));
            } catch (Exception e) {
                assert (false);
            }
        }
        */
    }
}
