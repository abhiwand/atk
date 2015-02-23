//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.graphbuilder.util;

import org.junit.Test;

import static junit.framework.Assert.*;


public class SerializableBaseConfigurationTest {

    @Test
    public void testEquality() throws Exception {
        SerializableBaseConfiguration config1 = new SerializableBaseConfiguration();
        config1.setProperty("key1", "value1");
        config1.setProperty("key2", 56);
        config1.setProperty("key3", false);

        SerializableBaseConfiguration config2 = new SerializableBaseConfiguration();
        config2.setProperty("key1", "value1");
        config2.setProperty("key2", 56);
        config2.setProperty("key3", false);

        assertTrue(config1.equals(config2));
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    public void testInEquality() throws Exception {
        SerializableBaseConfiguration config1 = new SerializableBaseConfiguration();
        config1.setProperty("key1", "value1");
        config1.setProperty("key2", 56);
        config1.setProperty("key3", false);

        SerializableBaseConfiguration config2 = new SerializableBaseConfiguration();
        config2.setProperty("key1", "notequal");
        config2.setProperty("key2", 56);
        config2.setProperty("key3", false);

        assertFalse(config1.equals(config2));
        assertNotSame(config1.hashCode(), config2.hashCode());

        assertFalse(config1.equals(null));

        assertFalse(config2.equals(new String("test")));
    }

}
