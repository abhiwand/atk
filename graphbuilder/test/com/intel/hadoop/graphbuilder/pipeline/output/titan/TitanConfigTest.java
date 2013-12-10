package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.util.RuntimeConfig;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;

public class TitanConfigTest {

    @Test
    public void testSingletonConfig() throws Exception {
        assertSame(TitanConfig.config, RuntimeConfig.getInstance(TitanConfig.class));

    }

    @Test
    public void testConstructor() throws Exception {

        // just a placeholder
        TitanConfig tc = new TitanConfig();

        assertNotNull(tc);
    }
}
