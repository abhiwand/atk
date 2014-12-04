package com.intel.graphbuilder.graph.titan

import com.google.common.cache.{ CacheBuilder, CacheLoader, RemovalListener, RemovalNotification }
import com.thinkaurelius.titan.core.{ TitanFactory, TitanGraph }
import org.apache.commons.configuration.Configuration

/**
 * This class caches Titan graphs so that multiple threads in a single JVM can share a Titan connection.
 *
 *  Instantiating multiple Titan graph instances in a single JVM is a significant bottleneck for Spark/GraphX
 *  because the cost of instantiating a Titan connection is high, and it also leads to increased contention among
 *  threads. Titan was implemented for Hadoop where the execution model is a single JVM per running task,
 *  However in Spark we have multiple running tasks per JVM. We need to cache graph connections to reduce
 *  contention among threads.
 */
class TitanGraphCacheOld() extends Serializable {

  /**
   * Creates a Titan graph for the corresponding Titan configuration if the graph does not exist in the cache.
   */
  val cacheLoader = new CacheLoader[Configuration, TitanGraph]() {
    def load(config: Configuration): TitanGraph = {
      TitanFactory.open(config)
    }
  }

  /**
   * Shut down a Titan graph when it is removed from the cache
   */
  val removalListener = new RemovalListener[Configuration, TitanGraph]() {
    def onRemoval(removal: RemovalNotification[Configuration, TitanGraph]) {
      val titanGraph = removal.getValue();
      titanGraph.shutdown()
    }
  }

  /**
   * Cache of Titan graphs
   */
  val cache = CacheBuilder
    .newBuilder()
    .weakValues()
    .recordStats()
    .removalListener(removalListener)
    .build(cacheLoader)

  /**
   * Get a Titan graph from the cache which matches the configuration.
   *
   * @param config Titan configuration
   * @return Titan graph
   */
  def getGraph(config: Configuration): TitanGraph = {
    val titanGraph = cache.getUnchecked(config)
    titanGraph
  }

  /**
   * Invalidate all entries in the cache
   */
  def invalidateAllCacheEntries: Unit = {
    cache.invalidateAll()
  }
}
