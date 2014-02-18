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

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.VertexID;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EdgeListFormatTest {

    @Test
    public void testToString_WithoutProperties() throws Exception {

        // setup test data
        Edge edge = createEdge("0001", "0002", "vertexLabel", "edgeLabel");
        edge.setProperties(createDummyPropertyMap());

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        String s = edgeListFormat.toString(edge, false);

        assertEquals("vertexLabel.0001\tvertexLabel.0002\tedgeLabel", s);
    }

    @Test
    public void testToStringWithoutProperties_WithLabels() throws Exception {

        // setup test data
        Edge edge = createEdge("0001", "0002", "vertexLabel", "edgeLabel");

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        String s = edgeListFormat.toStringWithoutProperties(edge);

        assertEquals("vertexLabel.0001\tvertexLabel.0002\tedgeLabel", s);
    }

    @Test
    public void testToStringWithoutProperties_AlternativeDelimiter() throws Exception {

        // setup test data
        Edge edge = createEdge("0001", "0002", "vertexLabel", "edgeLabel");

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat(",");

        // invoke method under test
        String s = edgeListFormat.toStringWithoutProperties(edge);

        assertEquals("vertexLabel.0001,vertexLabel.0002,edgeLabel", s);
    }

    @Test
    public void testToStringWithoutProperties_NoLabels() throws Exception {

        // setup test data
        Edge edge = createEdge("0001", "0002", null, "edgeLabel");

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        String s = edgeListFormat.toStringWithoutProperties(edge);

        assertEquals("0001\t0002\tedgeLabel", s);
    }

    @Test
    public void testToStringWithoutProperties_WithIgnoredPropertyMap() throws Exception {

        // setup test data
        Edge edge = createEdge("0001", "0002", "vertexLabel", "edgeLabel");
        edge.setProperties(createDummyPropertyMap());

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        String s = edgeListFormat.toStringWithoutProperties(edge);

        assertEquals("vertexLabel.0001\tvertexLabel.0002\tedgeLabel", s);
    }

    @Test
    public void testToStringWithProperties_OneProperty() throws Exception {

        // setup test data
        Edge edge = createEdge("0001", "0002", "vertexLabel", "edgeLabel");
        edge.setProperty("key", new StringType("myvalue"));

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        String s = edgeListFormat.toStringWithProperties(edge);

        assertEquals("vertexLabel.0001\tvertexLabel.0002\tedgeLabel\tkey\tmyvalue", s);
    }

    @Test
    public void testToStringWithProperties_TwoProperty() throws Exception {

        // setup test data
        Edge edge = createEdge("0001", "0002", "vertexLabel", "edgeLabel");
        edge.setProperty("key", new StringType("myvalue"));
        edge.setProperty("222", new StringType("second"));

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        String s = edgeListFormat.toStringWithProperties(edge);

        assertEquals("vertexLabel.0001\tvertexLabel.0002\tedgeLabel\t222\tsecond\tkey\tmyvalue", s);
    }

    @Test
    public void testToEdge_WithoutProperties() throws Exception {

        // setup test data
        String s = "vertexLabel.0001\tvertexLabel.0002\tedgeLabel\t222\tsecond\tkey\tmyvalue";

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        Edge edge = edgeListFormat.toEdge(s, false);

        assertEquals("0001", edge.getSrc().getName().toString());
        assertEquals("0002", edge.getDst().getName().toString());
        assertEquals("vertexLabel", edge.getSrc().getLabel().toString());
        assertEquals("vertexLabel", edge.getDst().getLabel().toString());
        assertEquals("edgeLabel", edge.getLabel().get());
        assertEquals(0, edge.getProperties().getPropertyKeys().size());
    }

    @Test
    public void testToEdge_WithoutProperties_LabelsHaveDots() throws Exception {

        // setup test data
        String s = "vertex.Label.0001\tvertex.Label.0002\tedgeLabel";

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        Edge edge = edgeListFormat.toEdge(s, false);

        assertEquals("0001", edge.getSrc().getName().toString());
        assertEquals("0002", edge.getDst().getName().toString());
        assertEquals("vertex.Label", edge.getSrc().getLabel().toString());
        assertEquals("vertex.Label", edge.getDst().getLabel().toString());
        assertEquals("edgeLabel", edge.getLabel().get());
        assertEquals(0, edge.getProperties().getPropertyKeys().size());
    }

    @Test
    public void testToEdge_WithProperties() throws Exception {

        // setup test data
        String s = "vertexLabel.0001\tvertexLabel.0002\tedgeLabel\t222\tsecond\tkey\tmyvalue";

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        Edge edge = edgeListFormat.toEdge(s, true);

        assertEquals("0001", edge.getSrc().getName().toString());
        assertEquals("0002", edge.getDst().getName().toString());
        assertEquals("vertexLabel", edge.getSrc().getLabel().toString());
        assertEquals("vertexLabel", edge.getDst().getLabel().toString());
        assertEquals("edgeLabel", edge.getLabel().get());

        assertEquals(2, edge.getProperties().getPropertyKeys().size());

        assertEquals("myvalue", edge.getProperty("key").toString());
        assertEquals("second", edge.getProperty("222").toString());
    }

    @Test
    public void testParseVertexID_LabelAndName() {

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        VertexID id = edgeListFormat.parseVertexID("label.name");

        assertEquals("label", id.getLabel().get());
        assertEquals("name", id.getName().toString());
    }

    @Test
    public void testParseVertexID_NameOnly() {

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        VertexID id = edgeListFormat.parseVertexID("name");

        assertNull(id.getLabel());
        assertEquals("name", id.getName().toString());
    }

    @Test
    public void testParseVertexID_LabelWithADot() {

        // initialize class under test
        EdgeListFormat edgeListFormat = new EdgeListFormat();

        // invoke method under test
        VertexID id = edgeListFormat.parseVertexID("[label.more].name");

        assertEquals("[label.more]", id.getLabel().get());
        assertEquals("name", id.getName().toString());
    }

    private Edge<StringType> createEdge(String srcName, String dstName, String vertexLabel, String edgeLabel) {
        return new Edge<StringType>(createId(srcName, vertexLabel), createId(dstName, vertexLabel), new StringType(edgeLabel));
    }

    private VertexID<StringType> createId(String name, String label) {
        if (label != null) {
            return new VertexID<StringType>(new StringType(name), new StringType(label));
        }
        else {
            return new VertexID<StringType>(new StringType(name));
        }
    }

    /** A property map with something in it, used for making sure nothing is included accidentally */
    private PropertyMap createDummyPropertyMap() {
        PropertyMap propertyMap = new PropertyMap();
        propertyMap.setProperty("ignored", new StringType("ignored"));
        return propertyMap;
    }
}
