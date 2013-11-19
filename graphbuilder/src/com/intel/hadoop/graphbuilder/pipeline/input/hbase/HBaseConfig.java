package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

import com.intel.hadoop.graphbuilder.util.RuntimeConfig;

public class HBaseConfig {
    public static final int    HBASE_CACHE_SIZE            = 500;

    public static RuntimeConfig config = RuntimeConfig.getInstance(HBaseConfig.class);
}
