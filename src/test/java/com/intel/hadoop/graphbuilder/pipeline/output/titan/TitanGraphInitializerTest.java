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
package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.thinkaurelius.titan.core.KeyMaker;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanType;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TitanGraphInitializerTest {

    @Test
    public void testCreateGbId() throws Exception {

        // initialize class under test
        TitanGraphInitializer initializer = new TitanGraphInitializer(null, null, null);

        // invoke method under test
        GBTitanKey key = initializer.createGbId();

        // assertions
        assertEquals(TitanConfig.GB_ID_FOR_TITAN, key.getName());
        assertTrue(key.isVertexIndex());
        assertTrue(key.isUnique());
        assertFalse(key.isEdgeIndex());
    }

    @Test
    public void testGetOrCreateTitanKey_TypeFound() throws Exception {
        String name = "someName";

        // setup mocks
        TitanType titanType = mock(TitanKey.class);
        TitanGraph graph = mock(TitanGraph.class);
        when(graph.getType(name)).thenReturn(titanType);

        // initialize class under test
        TitanGraphInitializer initializer = new TitanGraphInitializer(null, null, null);
        initializer.setGraph(graph);

        // invoke method under test
        TitanKey titanKey = initializer.getOrCreateTitanKey(new GBTitanKey(name));

        assertEquals(titanType, titanKey);
    }

    @Test
    public void testGetOrCreateTitanKey_Create() throws Exception {
        String keyName = "myname";

        // setup mocks
        TitanKey expectedTitanKey = mock(TitanKey.class);
        KeyMaker keyMaker = mock(KeyMaker.class);
        when(keyMaker.make()).thenReturn(expectedTitanKey);
        TitanGraph graph = mock(TitanGraph.class);
        when(graph.makeKey(keyName)).thenReturn(keyMaker);

        // initialize class under test
        TitanGraphInitializer initializer = new TitanGraphInitializer(null, null, null);
        initializer.setGraph(graph);

        // invoke method under test
        TitanKey actualTitanKey = initializer.getOrCreateTitanKey(new GBTitanKey(keyName));

        assertEquals(expectedTitanKey, actualTitanKey);
    }

    @Test
    public void testGetTitanKey_TypeFound() throws Exception {
        String name = "someName";

        // setup mocks
        TitanType titanType = mock(TitanKey.class);
        TitanGraph graph = mock(TitanGraph.class);
        when(graph.getType(name)).thenReturn(titanType);

        // initialize class under test
        TitanGraphInitializer initializer = new TitanGraphInitializer(null, null, null);
        initializer.setGraph(graph);

        // invoke method under test
        TitanKey titanKey = initializer.getTitanKey(name);

        assertEquals(titanType, titanKey);
    }

    @Test
    public void testGetTitanKey_TypeNotFound() throws Exception {
        String name = "someName";

        // setup mocks
        TitanGraph graph = mock(TitanGraph.class);

        // initialize class under test
        TitanGraphInitializer initializer = new TitanGraphInitializer(null, null, null);
        initializer.setGraph(graph);

        // invoke method under test
        TitanKey titanKey = initializer.getTitanKey(name);

        assertNull(titanKey);
    }

    @Test
    public void testCreateTitanKey_Simple() throws Exception {

        String keyName = "myname";

        // setup mocks
        KeyMaker keyMaker = mock(KeyMaker.class);
        TitanGraph graph = mock(TitanGraph.class);
        when(graph.makeKey(keyName)).thenReturn(keyMaker);

        // initialize class under test
        TitanGraphInitializer initializer = new TitanGraphInitializer(null, null, null);
        initializer.setGraph(graph);

        // invoke method under test
        initializer.createTitanKey(new GBTitanKey(keyName));

        // verify
        verify(keyMaker, times(1)).dataType(String.class);
        verify(keyMaker, times(0)).indexed(Vertex.class);
        verify(keyMaker, times(0)).indexed(Edge.class);
        verify(keyMaker, times(0)).unique();
        verify(keyMaker, times(1)).make();
        verifyNoMoreInteractions(keyMaker);
    }

    @Test
    public void testCreateTitanKey_UniqueVertex() throws Exception {

        String keyName = "myname";

        // setup mocks
        KeyMaker keyMaker = mock(KeyMaker.class);
        TitanGraph graph = mock(TitanGraph.class);
        when(graph.makeKey(keyName)).thenReturn(keyMaker);

        // initialize class under test
        TitanGraphInitializer initializer = new TitanGraphInitializer(null, null, null);
        initializer.setGraph(graph);

        // initialize test data
        GBTitanKey gbTitanKey = new GBTitanKey(keyName);
        gbTitanKey.setDataType(Integer.class);
        gbTitanKey.setIsVertexIndex(true);
        gbTitanKey.setIsUnique(true);


        // invoke method under test
        initializer.createTitanKey(gbTitanKey);

        // verify
        verify(keyMaker, times(1)).dataType(Integer.class);
        verify(keyMaker, times(1)).indexed(Vertex.class);
        verify(keyMaker, times(0)).indexed(Edge.class);
        verify(keyMaker, times(1)).unique();
        verify(keyMaker, times(1)).make();
        verifyNoMoreInteractions(keyMaker);
    }

    @Test
    public void testParseKeyCommandLine_Empty() throws Exception {

        // initialize class under test
        TitanGraphInitializer initializer = new TitanGraphInitializer(null, null, "");

        // invoke method under test
        List<GBTitanKey> gbTitanKeyList = initializer.parseKeyCommandLine();

        assertEquals(0, gbTitanKeyList.size());
    }

    @Test
    public void testParseKeyCommandLine() throws Exception {

        // initialize class under test
        TitanGraphInitializer initializer = new TitanGraphInitializer(null, null, "cf:userId;String;U;V,cf:eventId;E;Long");

        // invoke method under test
        List<GBTitanKey> list = initializer.parseKeyCommandLine();

        assertEquals(2, list.size());

        GBTitanKey userKey = list.get(0);
        assertEquals("cf:userId", userKey.getName());
        assertEquals(String.class, userKey.getDataType());
        assertTrue(userKey.isVertexIndex());
        assertFalse(userKey.isEdgeIndex());
        assertTrue(userKey.isUnique());

        GBTitanKey eventKey = list.get(1);
        assertEquals("cf:eventId", eventKey.getName());
        assertEquals(Long.class, eventKey.getDataType());
        assertFalse(eventKey.isVertexIndex());
        assertTrue(eventKey.isEdgeIndex());
        assertFalse(eventKey.isUnique());

    }
}
