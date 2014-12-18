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

package com.intel.graphbuilder.titan.cache;

import com.intel.graphbuilder.util.SerializableBaseConfiguration;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.titan_050.util.CachedTitanInputFormat;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.TITAN_INPUT_VERSION;

/**
 * Configuration for Titan/Hadoop graphs used to retrieve graphs from the Titan/Hadoop graph cache.
 * <p/>
 * The configuration object is used as the key to the graph caches, and retrieves Titan/Hadoop graphs
 * with the same set of Titan configuration properties.
 */
public class TitanHadoopCacheConfiguration {

    /**
     * Prefix that identifies Titan/Hadoop specific configurations
     */
    public static String TITAN_HADOOP_PREFIX = "titan.hadoop.input.conf.";

    /**
     * Titan configuration properties
     */
    private final SerializableBaseConfiguration titanConfig;

    /**
     * Titan/Hadoop configuration used to instantiate graph
     */
    private final ModifiableHadoopConfiguration faunusConf;

    /**
     * Titan/Hadoop input format class name
     */
    private final String inputFormatClassName;

    /**
     * Create cache configuration for Titan/Hadoop graphs
     *
     * @param faunusConf Titan/Hadoop (Faunus) configuration
     */
    public TitanHadoopCacheConfiguration(ModifiableHadoopConfiguration faunusConf) throws  IllegalArgumentException{
        if ( null == faunusConf) throw new IllegalArgumentException("Faunus configuration must not be null");

        this.faunusConf = faunusConf;
        this.inputFormatClassName = getInputFormatClassName();
        this.titanConfig = createTitanConfiguration(faunusConf.getHadoopConfiguration());
    }

    /**
     * Get the Titan/Hadoop (Faunus) configuration
     */
    public ModifiableHadoopConfiguration getFaunusConfiguration() {
        return faunusConf;
    }

    /**
     * Get the Titan configuration
     */
    public SerializableBaseConfiguration getTitanConfiguration() {
        return titanConfig;
    }


    /**
     * Get Titan input format class name
     */
    public String getInputFormatClassName() {
        String titanVersion = faunusConf.get(TITAN_INPUT_VERSION);
        String inputFormatClassName = CachedTitanInputFormat.SETUP_PACKAGE_PREFIX +
                titanVersion + CachedTitanInputFormat.SETUP_CLASS_NAME;
        return(inputFormatClassName);
    }

    /**
     * Compute the hashcode based on the Titan property values.
     * <p/>
     * The hashcode allows us to compare two configuration objects with the same Titan property entries.
     *
     * @return Hashcode based on property values
     */
    @Override
    public int hashCode() {
        return (this.titanConfig.hashCode());
    }

    /**
     * Configuration objects are considered equal if they contain the same Titan property keys and values.
     *
     * @param obj Configuration object to compare
     * @return True if the configuration objects have matching Titan property keys and values
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        TitanHadoopCacheConfiguration that = (TitanHadoopCacheConfiguration) obj;
        return (this.titanConfig.equals(that.getTitanConfiguration()));
    }

    /**
     * Create Titan configuration from Hadoop configuration
     *
     * @param hadoopConfig Hadoop configuration
     * @return Titan configuration
     */
    private SerializableBaseConfiguration createTitanConfiguration(Configuration hadoopConfig) {
        SerializableBaseConfiguration titanConfig = new SerializableBaseConfiguration();
        Iterator<Map.Entry<String, String>> itty = hadoopConfig.iterator();

        while (itty.hasNext()) {
            Map.Entry<String, String> entry = itty.next();
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.startsWith(TITAN_HADOOP_PREFIX)) {
                titanConfig.setProperty(key.substring(TITAN_HADOOP_PREFIX.length() + 1), value);
            }
        }
        return (titanConfig);
    }
}
