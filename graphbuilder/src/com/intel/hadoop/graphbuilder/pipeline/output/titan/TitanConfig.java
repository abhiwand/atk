package com.intel.hadoop.graphbuilder.pipeline.output.titan;


import com.intel.hadoop.graphbuilder.util.RuntimeConfig;

public class TitanConfig {

    public static final String TITAN_STORAGE_BACKEND            = "hbase";
    public static final String TITAN_STORAGE_HOSTNAME           = "localhost";
    public static final String TITAN_STORAGE_TABLENAME          = "titan";
    public static final String TITAN_STORAGE_PORT               = "2181";
    public static final String TITAN_STORAGE_CONNECTION_TIMEOUT = "10000";

    public static final RuntimeConfig config = RuntimeConfig.getInstance(TitanConfig.class);
}
