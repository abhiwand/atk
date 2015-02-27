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
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration
import org.apache.hadoop.conf.Configuration
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }

class TitanHadoopGraphCacheTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfter {
  val hadoopCacheConfiguration = mock[TitanHadoopCacheConfiguration]
  val hadoopConfig = new Configuration()

  before {
    hadoopConfig.set("titan.hadoop.input.conf.storage.backend", "inmemory")
    when(hadoopCacheConfiguration.getInputFormatClassName).thenReturn("com.thinkaurelius.titan.hadoop.formats.util.input.current.TitanHadoopSetupImpl")
    when(hadoopCacheConfiguration.getFaunusConfiguration).thenReturn(new ModifiableHadoopConfiguration(hadoopConfig))
  }

  "TitanHadoopGraphCache" should "create a Titan/Hadoop graph instance if it does not exist in the cache" in {
    val titanHadoopGraphCache = new TitanHadoopGraphCache()
    val config = new SerializableBaseConfiguration()
    config.setProperty("storage.backend", "inmemory")

    val titanHadoopGraph = titanHadoopGraphCache.getGraph(hadoopCacheConfiguration)

    titanHadoopGraph shouldNot equal(null)
    titanHadoopGraphCache.cache.size() shouldEqual (1)
  }
  "TitanHadoopGraphCache" should "retrieve a Titan/Hadoop graph instance if it already exists in the cache" in {
    val titanHadoopGraphCache = new TitanHadoopGraphCache()

    val titanHadoopGraph = titanHadoopGraphCache.getGraph(hadoopCacheConfiguration)
    val titanHadoopGraph2 = titanHadoopGraphCache.getGraph(hadoopCacheConfiguration)

    titanHadoopGraph should equal(titanHadoopGraph2)
    titanHadoopGraphCache.cache.size() shouldEqual (1)
  }
  "TitanHadoopGraphCache" should "invalidate cache entries" in {
    val titanHadoopGraphCache = new TitanHadoopGraphCache()

    val titanHadoopGraph = titanHadoopGraphCache.getGraph(hadoopCacheConfiguration)
    titanHadoopGraph shouldNot equal(null)
    titanHadoopGraphCache.cache.size() shouldEqual (1)

    titanHadoopGraphCache.invalidateAllCacheEntries()
    titanHadoopGraphCache.cache.size() shouldEqual (0)

  }
  "TitanHadoopGraphCache" should "throw an IllegalArgumentException if hadoop configuration is null" in {
    val titanHadoopGraphCache = new TitanHadoopGraphCache()
    intercept[IllegalArgumentException] {
      titanHadoopGraphCache.getGraph(null)
    }
  }

}
