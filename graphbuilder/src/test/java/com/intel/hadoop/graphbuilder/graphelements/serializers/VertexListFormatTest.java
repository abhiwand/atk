/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.graphelements.serializers;

import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class VertexListFormatTest {

    @Test
    public void testToString_WithoutProperties() throws Exception {
        // setup test data
        String name = "someName";
        String label = "someLabel";
        Vertex vertex = createVertex(name, label, null);

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        String s = vertexListFormat.toString(vertex, false);

        assertEquals("someName\tsomeLabel", s);
    }

    @Test
    public void testToString_WithProperties() throws Exception {

        // setup test data
        String name = "someName";
        String label = "someLabel";
        PropertyMap propertyMap = new PropertyMap();
        propertyMap.setProperty("one", new StringType("1111"));
        propertyMap.setProperty("two", new StringType("2222"));
        Vertex vertex = createVertex(name, label, propertyMap);

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        String s = vertexListFormat.toString(vertex, true);

        assertEquals("someName\tsomeLabel\ttwo\t2222\tone\t1111", s);
    }

    @Test
    public void testToStringWithProperties_OneProperty() throws Exception {

        // setup test data
        String name = "someName";
        String label = "someLabel";
        PropertyMap propertyMap = new PropertyMap();
        propertyMap.setProperty("one", new StringType("1111"));
        Vertex vertex = createVertex(name, label, propertyMap);

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        String s = vertexListFormat.toStringWithProperties(vertex);

        assertEquals("someName\tsomeLabel\tone\t1111", s);
    }

    @Test
    public void testToStringWithProperties_TwoProperties() throws Exception {

        // setup test data
        String name = "someName";
        String label = "someLabel";
        PropertyMap propertyMap = new PropertyMap();
        propertyMap.setProperty("one", new StringType("1111"));
        propertyMap.setProperty("two", new StringType("2222"));
        Vertex vertex = createVertex(name, label, propertyMap);

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        String s = vertexListFormat.toStringWithProperties(vertex);

        assertEquals("someName\tsomeLabel\ttwo\t2222\tone\t1111", s);
    }

    @Test
    public void testToStringWithProperties_NullPropertyMap() throws Exception {

        // setup test data
        String name = "someName";
        String label = "someLabel";
        Vertex vertex = createVertex(name, label, null);

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        String s = vertexListFormat.toStringWithProperties(vertex);

        assertEquals("someName\tsomeLabel", s);
    }

    @Test
    public void testToStringWithProperties_EmptyPropertyMap() throws Exception {

        // setup test data
        String name = "someName";
        String label = "someLabel";
        Vertex vertex = createVertex(name, label, createEmptyPropertyMap());

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        String s = vertexListFormat.toStringWithProperties(vertex);

        assertEquals("someName\tsomeLabel", s);
    }

    @Test
    public void testToStringWithoutProperties_OtherDelimiter() throws Exception {

        // setup test data
        String delimiter = "#";
        String name = "someName";
        String label = "someLabel";
        Vertex vertex = createVertex(name, label, null);

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat(delimiter);

        // invoke method under test
        String s = vertexListFormat.toStringWithoutProperties(vertex);

        assertEquals("someName#someLabel", s);
    }

    @Test
    public void testToStringWithoutProperties_NullMap() throws Exception {

        // setup test data
        String name = "someName";
        String label = "someLabel";
        Vertex vertex = createVertex(name, label, null);

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        String s = vertexListFormat.toStringWithoutProperties(vertex);

        assertEquals("someName\tsomeLabel", s);
    }

    @Test
    public void testToStringWithoutProperties_DummyMap() throws Exception {

        // setup test data
        String name = "someName";
        String label = "someLabel";
        Vertex vertex = createVertex(name, label, createDummyPropertyMap());

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        String s = vertexListFormat.toStringWithoutProperties(vertex);

        assertEquals("someName\tsomeLabel", s);
    }

    @Test
    public void testToStringWithoutProperties_NullLabel() throws Exception {

        // setup test data
        String name = "someName";
        String label = null;
        Vertex vertex = createVertex(name, label, createDummyPropertyMap());

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        String s = vertexListFormat.toStringWithoutProperties(vertex);

        assertEquals("someName", s);
    }

    @Test
    public void testToStringWithoutProperties_LongTypeName() throws Exception {

        // setup test data
        Long name = 1L;
        String label = "someLabel";
        Vertex vertex = new Vertex<LongType>(new LongType(name));
        vertex.setLabel(new StringType(label));

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        String s = vertexListFormat.toStringWithoutProperties(vertex);

        assertEquals("1\tsomeLabel", s);
    }


    @Test
    public void testToVertexWithProperties() throws Exception {
        // setup test data
        String s = "someName\tsomeLabel\tkey\tvalue";

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        Vertex vertex = vertexListFormat.toVertex(s, true);

        assertEquals("someName", vertex.getId().getName().toString());
        assertEquals("someLabel", vertex.getLabel().toString());
        assertEquals(1, vertex.getProperties().getPropertyKeys().size());
        assertEquals("value", vertex.getProperty("key").toString());
    }

    @Test
    public void testToVertex_WithoutProperties() throws Exception {

        // setup test data
        String s = "someName\tsomeLabel";

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        Vertex vertex = vertexListFormat.toVertex(s, false);

        assertEquals("someName", vertex.getId().getName().toString());
        assertEquals("someLabel", vertex.getLabel().toString());
    }

    @Test
    public void testToVertex_WithoutPropertiesNullLabel() throws Exception {

        // setup test data
        String s = "someName";

        // initialize class under test
        VertexListFormat vertexListFormat = new VertexListFormat();

        // invoke method under test
        Vertex vertex = vertexListFormat.toVertex(s, false);

        assertEquals("someName", vertex.getId().getName().toString());
        assertNull(vertex.getLabel());
    }

    private Vertex createVertex(String name, String label, PropertyMap propertyMap) {
        Vertex vertex = new Vertex<StringType>(new StringType(name));
        if (label != null) {
            vertex.setLabel(new StringType(label));
        }
        vertex.setProperties(propertyMap);
        return vertex;
    }

    /** A property map with something in it, used for making sure nothing is included accidentally */
    private PropertyMap createDummyPropertyMap() {
        PropertyMap propertyMap = new PropertyMap();
        propertyMap.setProperty("ignored", new StringType("ignored"));
        return propertyMap;
    }

    private PropertyMap createEmptyPropertyMap() {
        PropertyMap propertyMap = new PropertyMap();
        return propertyMap;
    }

}
