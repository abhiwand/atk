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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;

/**
 * Abstract class for caching Titan graphs so that multiple threads in a single JVM can share a Titan connection.
 * <p/>
 * Instantiating multiple Titan graph instances in a single JVM is a significant bottleneck for Spark/GraphX/Giraph
 * because the cost of instantiating a Titan connection is high, and it also leads to increased contention among
 * threads.
 * <p/>
 * This abstract class serves as a base implementation for standard Titan graphs and Titan Hadoop graphs.
 */
public abstract class AbstractTitanGraphCache<K, V> implements Serializable {

    private final Log LOG = LogFactory.getLog(AbstractTitanGraphCache.class);

    /**
     * Cache of Titan graphs
     */
    public LoadingCache<K, V> cache;

    /**
     * Create Titan graph cache
     *
     * @param cacheLoader     Cache loader with a factory method to create a graph
     * @param removalListener Removal listener which shuts down the graph when entries are evicted from cache
     * @return Titan graph cache
     */
    public LoadingCache<K, V> createCache(CacheLoader<K, V> cacheLoader,
                                          RemovalListener<K, V> removalListener) {
        LOG.info("Creating Titan graph cache");
        LoadingCache<K, V> cache = CacheBuilder
                .newBuilder()
                .weakValues()
                .recordStats()
                .removalListener(removalListener)
                .build(cacheLoader);
        return (cache);
    }

    /**
     * Get a Titan graph from the cache which matches the configuration key
     *
     * @param config Titan configuration
     * @return Titan graph
     */
    public V getGraph(K config) throws IllegalArgumentException {
        if ( null == config) throw new IllegalArgumentException("Configuration must not be null");

        V titanGraph = cache.getUnchecked(config);
        LOG.info("Getting Titan graph from cache: " + cache.stats());
        return (titanGraph);
    }

    /**
     * Invalidate all entries in the cache
     */
    public void invalidateAllCacheEntries() {
        cache.invalidateAll();
        LOG.info("Invalidating Titan graph cache: " + cache.stats());
    }

}
