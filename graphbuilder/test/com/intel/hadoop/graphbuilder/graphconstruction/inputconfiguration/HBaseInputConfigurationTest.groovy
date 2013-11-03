package com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration

import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.HBaseReaderMapper

import org.junit.Test

import static junit.framework.Assert.assertEquals
import static junit.framework.Assert.assertNotNull
import static junit.framework.Assert.assertNotNull
import static junit.framework.Assert.assertNotSame

class HBaseInputConfigurationTest {

    @Test
    public void testConstructor() {

        HBaseInputConfiguration configuration = new HBaseInputConfiguration();

        assertNotNull(configuration)
        assert(configuration.usesHBase())
        assertEquals(configuration.getMapperClass(),HBaseReaderMapper.class)
        assertNotNull(configuration.getDescription())
        assert(configuration.getDescription().length() > 0)
    }


}
