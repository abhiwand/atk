//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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
package com.intel.giraph.io.titan;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import java.util.Iterator;
import java.util.Map;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BATCH_LOADING;

/**
 * Converts a Giraph configuration file to a Titan configuration file. For all
 * Titan specific properties, the conversion removes the Giraph prefix and
 * provides to Titan's graph factory.
 */
public class GiraphToTitanGraphFactory {
    /**
     * prevent instantiation of utilize class
     */
    private GiraphToTitanGraphFactory() {
    }

    /**
     * generateTitanConfiguration from Giraph configuration
     *
     * @param hadoopConfig : Giraph configuration
     * @param prefix : prefix to remove for Titan
     * @return BaseConfiguration
     */
    public static BaseConfiguration createTitanBaseConfiguration(Configuration hadoopConfig, String prefix) {

        BaseConfiguration titanConfig = new BaseConfiguration();
        Iterator<Map.Entry<String, String>> itty = hadoopConfig.iterator();

        while (itty.hasNext()) {
            Map.Entry<String, String> entry = itty.next();
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.startsWith(prefix)) {
                titanConfig.setProperty(key.substring(prefix.length() + 1), value);
            }
        }
        return titanConfig;
    }


    public static void addFaunusInputConfiguration(Configuration hadoopConfig) {
        BaseConfiguration titanConfig = createTitanBaseConfiguration(hadoopConfig, GIRAPH_TITAN.get(hadoopConfig));
        Iterator keys = titanConfig.getKeys();
        String prefix = "titan.hadoop.input.conf.";
        while (keys.hasNext())
        {
            String key = (String) keys.next();
            hadoopConfig.set(prefix + key, titanConfig.getString(key));
        }
    }
}
