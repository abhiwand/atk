package com.intel.graphbuilder.titan.cache

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import org.scalatest.{ FlatSpec, Matchers }

class TitanGraphCacheTest extends FlatSpec with Matchers {

  "TitanGraphCache" should "create a Titan graph instance if it does not exist in the cache" in {
    val titanGraphCache = new TitanGraphCache()
    val config = new SerializableBaseConfiguration()
    config.setProperty("storage.backend", "inmemory")

    val titanGraph = titanGraphCache.getGraph(config)

    titanGraph shouldNot equal(null)
    titanGraphCache.cache.size() shouldEqual (1)
  }
  "TitanGraphCache" should "retrieve a Titan graph instance if it already exists in the cache" in {
    val titanGraphCache = new TitanGraphCache()
    val config = new SerializableBaseConfiguration()
    config.setProperty("storage.backend", "inmemory")

    val titanGraph = titanGraphCache.getGraph(config)
    val titanGraph2 = titanGraphCache.getGraph(config)

    titanGraph should equal(titanGraph2)
    titanGraphCache.cache.size() shouldEqual (1)
  }
  "TitanGraphCache" should "invalidate cache entries" in {
    val titanGraphCache = new TitanGraphCache()
    val config = new SerializableBaseConfiguration()
    config.setProperty("storage.backend", "inmemory")

    val titanGraph = titanGraphCache.getGraph(config)
    titanGraph shouldNot equal(null)
    titanGraphCache.cache.size() shouldEqual (1)

    titanGraphCache.invalidateAllCacheEntries()
    titanGraph.isClosed should equal(true)
    titanGraphCache.cache.size() shouldEqual (0)

  }

}
