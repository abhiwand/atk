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
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class caches standard Titan graphs so that multiple threads in a single JVM can share a Titan connection.
 * <p/>
 * Instantiating multiple Titan graph instances in a single JVM is a significant bottleneck for Spark/GraphX/Giraph
 * because the cost of instantiating a Titan connection is high, and it also leads to increased contention among
 * threads.
 */
public class TitanGraphCache extends AbstractTitanGraphCache<Configuration, TitanGraph> {

    private final Log LOG = LogFactory.getLog(TitanGraphCache.class);

    /**
     * Creates a cache for standard Titan graphs.
     * <p/>
     * The cache is implemented as a key-value pair of configuration objects and standard Titan graphs.
     * Configuration objects are considered equal if they contain the same set of properties.
     */
    public TitanGraphCache() {
        LOG.info("Creating cache for standard Titan graphs");
        CacheLoader<Configuration, TitanGraph> cacheLoader = createCacheLoader();
        RemovalListener<Configuration, TitanGraph> removalListener = createRemovalListener();
        this.cache = createCache(cacheLoader, removalListener);
    }

    /**
     * Creates a Titan graph for the corresponding Titan configuration if the graph does not exist in the cache.
     */
    private CacheLoader<Configuration, TitanGraph> createCacheLoader() {
        CacheLoader<Configuration, TitanGraph> cacheLoader = new CacheLoader<Configuration, TitanGraph>() {
            public TitanGraph load(Configuration config) {
                LOG.info("Loading a standard titan graph into the cache");
                return TitanFactory.open(config);
            }
        };
        return (cacheLoader);
    }

    /**
     * Shut down a Titan graph when it is evicted from the cache
     */
    private RemovalListener<Configuration, TitanGraph> createRemovalListener() {
        RemovalListener<Configuration, TitanGraph> removalListener = new RemovalListener<Configuration, TitanGraph>() {
            public void onRemoval(RemovalNotification<Configuration, TitanGraph> removal) {
                TitanGraph titanGraph = removal.getValue();
                if (titanGraph != null) {
                    LOG.info("Evicting a standard Titan graph from the cache: " + cache.stats());
                    titanGraph.shutdown();
                }
            }
        };
        return (removalListener);
    }

}
