package com.intel.hadoop.graphbuilder.pipeline.output.titan;


import com.intel.hadoop.graphbuilder.util.RuntimeConfig;

import java.util.HashMap;

public class TitanConfig {

    private static HashMap<String, String> defaultConfigMap  = new HashMap<>();
    static {
        // Default Titan configuration for Graphbuilder
        defaultConfigMap.put("TITAN_STORAGE_BACKEND",           "hbase");
        defaultConfigMap.put("TITAN_STORAGE_HOSTNAME",          "localhost");
        defaultConfigMap.put("TITAN_STORAGE_TABLENAME",         "titan");
        defaultConfigMap.put("TITAN_STORAGE_PORT",              "2181");
        defaultConfigMap.put("TITAN_STORAGE_CONNECTION-TIMEOUT","10000");
        defaultConfigMap.put("TITAN_STORAGE_BATCH-LOADING",     "true");
        defaultConfigMap.put("TITAN_IDS_BLOCK-SIZE",            "100000");
    }

    public static RuntimeConfig config = RuntimeConfig.getInstanceWithDefaultConfig(defaultConfigMap);

}
