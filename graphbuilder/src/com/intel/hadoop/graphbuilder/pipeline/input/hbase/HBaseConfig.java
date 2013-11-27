package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

import com.intel.hadoop.graphbuilder.util.RuntimeConfig;

/**
 * Hold all the hbase default configs that can later be overwritten by a config file
 *
 */
public class HBaseConfig {
    /**
     * used to set Scan objects row caching
     * @see org.apache.hadoop.hbase.client.Scan
     */
    public static final int    HBASE_CACHE_SIZE            = 500;

    public static RuntimeConfig config = RuntimeConfig.getInstance(HBaseConfig.class);
}
