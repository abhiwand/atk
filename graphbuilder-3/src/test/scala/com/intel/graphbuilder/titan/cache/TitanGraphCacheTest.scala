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
    titanGraph.isOpen should equal(false)
    titanGraphCache.cache.size() shouldEqual (0)

  }
  "TitanGraphCache" should "throw an IllegalArgumentException if hadoop configuration is null" in {
    val titanGraphCache = new TitanGraphCache()
    intercept[IllegalArgumentException] {
      titanGraphCache.getGraph(null)
    }
  }
}
