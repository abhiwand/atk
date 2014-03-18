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

package com.intel.hadoop.graphbuilder.util;

import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the com.intel.hadoop.graphbuilder.util class <code>MultiValuedMap</code>
 */
public class MultiValuedMapTest {

    private static final String KEY1 = "Ce n'est pas une clé";
    private static final String KEY2 = "To the kingdom";

    private static final String value1 = "Family";
    private static final String value2 = "Heartland";

    @Test
    public void testAddGet_oneKeyOneValue() {

        MultiValuedMap map = new MultiValuedMap();

        map.add(KEY1,value1);

        Set<String> values = map.getValues(KEY1);

        assertNotNull(values);
        assertEquals(values.size(), 1);
        assertTrue(values.contains(value1));

        Set<String> keys = map.keySet();

        assertNotNull(keys);
        assertEquals(keys.size(), 1);
        assertTrue(keys.contains(KEY1));

    }

    @Test
    public void testAddGet_oneKeyTwoValues() {

        MultiValuedMap map = new MultiValuedMap();

        map.add(KEY1,value1);
        map.add(KEY1, value2);

        Set<String> values = map.getValues(KEY1);

        assertNotNull(values);
        assertEquals(values.size(), 2);
        assertTrue(values.contains(value1));
        assertTrue(values.contains(value2));

        Set<String> keys = map.keySet();

        assertNotNull(keys);
        assertEquals(keys.size(), 1);
        assertTrue(keys.contains(KEY1));
    }


    @Test
    public void testAddGet_keyNotAdded() {

        MultiValuedMap map = new MultiValuedMap();

        Set<String> values = map.getValues(KEY1);

        assertNull(values);

        Set<String> keys = map.keySet();

        assertNotNull(keys);
        assertEquals(keys.size(), 0);
    }

    @Test
    public void testAddGet_twoKeysOneValueEach() {

        MultiValuedMap map = new MultiValuedMap();

        map.add(KEY1, value1);
        map.add(KEY2, value2);

        Set<String> values1 = map.getValues(KEY1);
        Set<String> values2 = map.getValues(KEY2);


        assertNotNull(values1);
        assertEquals(values1.size(), 1);
        assertTrue(values1.contains(value1));

        assertNotNull(values2);
        assertEquals(values2.size(), 1);
        assertTrue(values2.contains(value2));

        Set<String> keys = map.keySet();

        assertNotNull(keys);
        assertEquals(keys.size(), 2);
        assertTrue(keys.contains(KEY1));
        assertTrue(keys.contains(KEY2));
    }

    @Test
    public void test_tryToAddAKeyTwice() {
        MultiValuedMap map = new MultiValuedMap();

        map.addKey(KEY1);
        boolean test = map.addKey(KEY1);

        assertFalse(test);

        Set<String> values = map.getValues(KEY1);

        assertNotNull(values);
        assertEquals(values.size(), 0);

        Set<String> keys = map.keySet();

        assertNotNull(keys);
        assertEquals(keys.size(), 1);
        assertTrue(keys.contains(KEY1));
    }

    @Test
    public void test_addKeyWontWipeOutYourSet() {
        MultiValuedMap map = new MultiValuedMap();

        map.add(KEY1, value1);
        boolean test = map.addKey(KEY1);

        assertFalse(test);

        Set<String> values = map.getValues(KEY1);

        assertNotNull(values);
        assertEquals(values.size(), 1);
        assertTrue(values.contains(value1));

        Set<String> keys = map.keySet();

        assertNotNull(keys);
        assertEquals(keys.size(), 1);
        assertTrue(keys.contains(KEY1));
    }
}
