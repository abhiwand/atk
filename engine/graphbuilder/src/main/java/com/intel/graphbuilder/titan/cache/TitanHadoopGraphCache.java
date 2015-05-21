//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * This class caches Titan/Hadoop graphs so that multiple threads in a single JVM can share a Titan connection.
 * <p/>
 * Titan/Hadoop graphs are used when reading Titan graphs from the backend storage using Hadoop input formats.
 * Instantiating multiple Titan graph instances in a single JVM is a significant bottleneck for Spark/GraphX/Giraph
 * because the cost of instantiating a Titan connection is high, and it also leads to increased contention among
 * threads.
 */
public class TitanHadoopGraphCache extends AbstractTitanGraphCache<TitanHadoopCacheConfiguration, TitanHadoopSetup> {

    private final Log LOG = LogFactory.getLog(TitanGraphCache.class);

    /**
     * Creates a cache for Titan/Hadoop graphs.
     * <p/>
     * The cache is implemented as a key-value pair of configuration objects and Titan/Hadoop graphs.
     * Configuration objects are considered equal if they contain the same set of properties.
     * Titan Hadoop graphs are instantiated in the TitanHadoopSetup object.
     */
    public TitanHadoopGraphCache() {
        LOG.info("Creating cache for Titan/Hadoop graphs");
        CacheLoader<TitanHadoopCacheConfiguration, TitanHadoopSetup> cacheLoader = createCacheLoader();
        RemovalListener<TitanHadoopCacheConfiguration, TitanHadoopSetup> removalListener = createRemovalListener();
        this.cache = createCache(cacheLoader, removalListener);
    }

    /**
     * Creates a Titan/Hadoop graph for the corresponding Titan configuration if the graph does not exist in the cache.
     */
    private CacheLoader<TitanHadoopCacheConfiguration, TitanHadoopSetup> createCacheLoader() {
        CacheLoader<TitanHadoopCacheConfiguration, TitanHadoopSetup> cacheLoader = new CacheLoader<TitanHadoopCacheConfiguration, TitanHadoopSetup>() {
            public TitanHadoopSetup load(TitanHadoopCacheConfiguration config) {
                LOG.info("Loading a Titan/Hadoop graph into the cache.");
                //Commenting Titan 0.5.2 API
                //return config.getTitanInputFormat().getGraphSetup();

                //Graph setup for Titan 0.5.1
                String inputFormatClassName = config.getInputFormatClassName();
                ModifiableHadoopConfiguration faunusConf = config.getFaunusConfiguration();
                TitanHadoopSetup titanHadoopSetup = ConfigurationUtil.instantiate(inputFormatClassName, new Object[]{faunusConf.getHadoopConfiguration()},
                        new Class[]{Configuration.class});
                return (titanHadoopSetup);
            }
        };
        return (cacheLoader);
    }


    /**
     * Shut down a Titan/Hadoop graph when it is removed from the cache
     */
    private RemovalListener<TitanHadoopCacheConfiguration, TitanHadoopSetup> createRemovalListener() {
        RemovalListener<TitanHadoopCacheConfiguration, TitanHadoopSetup> removalListener = new RemovalListener<TitanHadoopCacheConfiguration, TitanHadoopSetup>() {
            public void onRemoval(RemovalNotification<TitanHadoopCacheConfiguration, TitanHadoopSetup> removal) {
                TitanHadoopSetup titanGraph = removal.getValue();
                if (titanGraph != null) {
                    LOG.info("Evicting a Titan/Hadoop graph from the cache: " + cache.stats());
                    titanGraph.close();
                }
            }
        };
        return (removalListener);
    }


}
