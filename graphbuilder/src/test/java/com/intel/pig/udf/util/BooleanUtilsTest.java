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
package com.intel.pig.udf.util;

import org.junit.Assert;
import org.junit.Test;


public class BooleanUtilsTest {

    @Test(expected = IllegalArgumentException.class)
    public void testValidateBooleanString_Invalid() throws Exception {
        BooleanUtils.validateBooleanString("invalid");
    }

    @Test
    public void testValidateBooleanString_Valid() throws Exception {
        BooleanUtils.validateBooleanString("1");
        BooleanUtils.validateBooleanString("true");
        BooleanUtils.validateBooleanString("TRUE");
        BooleanUtils.validateBooleanString("True");
        BooleanUtils.validateBooleanString("0");
        BooleanUtils.validateBooleanString("false");
        BooleanUtils.validateBooleanString("FALSE");
        BooleanUtils.validateBooleanString("False");
    }

    @Test
    public void testToBoolean_True() throws Exception {
        Assert.assertTrue(BooleanUtils.toBoolean("1"));
        Assert.assertTrue(BooleanUtils.toBoolean("true"));
        Assert.assertTrue(BooleanUtils.toBoolean("TRUE"));
        Assert.assertTrue(BooleanUtils.toBoolean("True"));
    }

    @Test
    public void testToBoolean_False() throws Exception {
        Assert.assertFalse(BooleanUtils.toBoolean("0"));
        Assert.assertFalse(BooleanUtils.toBoolean("false"));
        Assert.assertFalse(BooleanUtils.toBoolean("FALSE"));
        Assert.assertFalse(BooleanUtils.toBoolean("False"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToBoolean_Invalid() throws Exception {
        BooleanUtils.toBoolean("invalid");
    }
}
