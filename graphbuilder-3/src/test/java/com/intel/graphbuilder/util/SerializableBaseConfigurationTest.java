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