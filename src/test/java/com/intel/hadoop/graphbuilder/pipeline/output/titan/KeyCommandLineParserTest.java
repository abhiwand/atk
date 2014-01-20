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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KeyCommandLineParserTest {

    @Test
    public void testParse_Empty() throws Exception {

        // invoke method under test
        List<GBTitanKey> gbTitanKeyList = new KeyCommandLineParser().parse("");

        assertEquals(0, gbTitanKeyList.size());
    }

    @Test
    public void testParse_WithOne() throws Exception {

        // invoke method under test
        List<GBTitanKey> list = new KeyCommandLineParser().parse("cf:userId;String;U;V");

        assertEquals(1, list.size());

        GBTitanKey userKey = list.get(0);
        assertEquals("cf:userId", userKey.getName());
        assertEquals(String.class, userKey.getDataType());
        assertTrue(userKey.isVertexIndex());
        assertFalse(userKey.isEdgeIndex());
        assertTrue(userKey.isUnique());
    }

    @Test
    public void testParse_WithTwo() throws Exception {

        // invoke method under test
        List<GBTitanKey> list = new KeyCommandLineParser().parse("cf:userId;String;U;V,cf:eventId;E;Long");

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

    @Test
    public void testParse_UniqueMustBeVertex() throws Exception {

        // invoke method under test
        List<GBTitanKey> list = new KeyCommandLineParser().parse("name;U");

        GBTitanKey userKey = list.get(0);
        assertTrue(userKey.isVertexIndex());
        assertTrue(userKey.isUnique());
    }
}
