
package com.intel.graphbuilder.titan.cache;

import com.intel.graphbuilder.util.SerializableBaseConfiguration;
import com.thinkaurelius.titan.hadoop.formats.util.TitanInputFormat;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

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
     * Titan/Hadoop input format used to retrieve configuration, and instantiate graph
     */
    private final TitanInputFormat titanInputFormat;

    /**
     * Create cache configuration for Titan/Hadoop graphs
     *
     * @param titanInputFormat Titan/Hadoop input format
     */
    public TitanHadoopCacheConfiguration(TitanInputFormat titanInputFormat) {
        this.titanInputFormat = titanInputFormat;
        this.titanConfig = createTitanConfiguration(titanInputFormat.getConf());
    }

    /**
     * Create Titan configuration from Hadoop configuration
     *
     * @param hadoopConfig Hadoop configuration
     * @return Titan configuration
     */
    public SerializableBaseConfiguration createTitanConfiguration(Configuration hadoopConfig) {
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

    /**
     * Get the Titan/Hadoop input format
     */
    public TitanInputFormat getTitanInputFormat() {
        return titanInputFormat;
    }

    /**
     * Get the Titan configuration
     */
    public SerializableBaseConfiguration getTitanConfiguration() {
        return titanConfig;
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

}
