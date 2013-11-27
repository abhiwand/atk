package com.intel.hadoop.graphbuilder.pipeline;

import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;

public class GraphConstructionPipelineTest {
    @Test
    public void testAddUserOpt() throws Exception {
        GraphConstructionPipeline pipeline = new GraphConstructionPipeline();

        String key   = "key";
        String value = "value";

        pipeline.addUserOpt(key, value);

        Field privateOptField = GraphConstructionPipeline.class.
                getDeclaredField("userOpts");

        privateOptField.setAccessible(true);

        HashMap opts = (HashMap) privateOptField.get(pipeline);
        assert(value.equals(opts.get(key)));
    }

    @Test
    public void testRun() throws Exception {

    }
}
