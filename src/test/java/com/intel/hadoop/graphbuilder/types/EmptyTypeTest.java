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
package com.intel.hadoop.graphbuilder.types;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

import java.io.DataInput;
import java.io.DataInputStream;

public class EmptyTypeTest {

    @Test
    public void testEmptyString() throws Exception {
        EmptyType emptyType = new EmptyType();
        String emptyString = emptyType.toString();
        assertEquals(emptyString, "");
    }

    @Test
    public void testGetBaseObject() throws Exception {
        EmptyType emptyType = new EmptyType();
        assertEquals(emptyType.getBaseObject(), EmptyType.INSTANCE);
    }

    @Test
    public void testAdd() throws Exception {
        EmptyType emptyType_0 = new EmptyType();
        EmptyType emptyType_1 = new EmptyType();
        EmptyType emptyType_2 = emptyType_0;

        emptyType_0.add(emptyType_1);
        assertEquals(emptyType_0, emptyType_2);
    }
}
