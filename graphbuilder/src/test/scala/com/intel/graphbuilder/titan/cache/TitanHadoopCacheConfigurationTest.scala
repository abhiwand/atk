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

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.TITAN_INPUT_VERSION
import org.apache.hadoop.conf.Configuration
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }

class TitanHadoopCacheConfigurationTest extends FlatSpec with Matchers with BeforeAndAfter {
  val hadoopConfig = new Configuration()
  val faunusConf = null;

  "TitanHadoopCacheConfiguration" should "create a TitanHadoopCacheConfiguration" in {
    val hadoopConfig = new Configuration()
    hadoopConfig.set("titan.hadoop.input.conf.storage.backend", "inmemory")

    val faunusConf = new ModifiableHadoopConfiguration(hadoopConfig)
    faunusConf.set(TITAN_INPUT_VERSION, "current")

    val titanHadoopCacheConf = new TitanHadoopCacheConfiguration(faunusConf)

    titanHadoopCacheConf.getFaunusConfiguration() should equal(faunusConf)
    titanHadoopCacheConf.getInputFormatClassName() should equal("com.thinkaurelius.titan.hadoop.formats.util.input.current.TitanHadoopSetupImpl")
  }

  "TitanHadoopCacheConfiguration" should "equal a TitanHadoopCacheConfiguration with the same Titan properties" in {
    val hadoopConfig = new Configuration()
    hadoopConfig.set("titan.hadoop.input.conf.storage.backend", "inmemory")
    hadoopConfig.set("titan.hadoop.input.conf.storage.batch-loading", "true")

    val faunusConf = new ModifiableHadoopConfiguration(hadoopConfig)
    faunusConf.set(TITAN_INPUT_VERSION, "current")

    val titanHadoopCacheConf = new TitanHadoopCacheConfiguration(faunusConf)
    val titanHadoopCacheConf2 = new TitanHadoopCacheConfiguration(faunusConf)

    titanHadoopCacheConf should equal(titanHadoopCacheConf2)
    titanHadoopCacheConf.hashCode() should equal(titanHadoopCacheConf2.hashCode())
  }

  "TitanHadoopCacheConfiguration" should "not equal a TitanHadoopCacheConfiguration with different Titan properties" in {
    val hadoopConfig = new Configuration()
    hadoopConfig.set("titan.hadoop.input.conf.storage.backend", "inmemory")
    hadoopConfig.set("titan.hadoop.input.conf.storage.batch-loading", "true")
    val faunusConf = new ModifiableHadoopConfiguration(hadoopConfig)
    faunusConf.set(TITAN_INPUT_VERSION, "current")

    val titanHadoopCacheConf = new TitanHadoopCacheConfiguration(faunusConf)

    val hadoopConfig2 = new Configuration()
    hadoopConfig2.set("titan.hadoop.input.conf.storage.backend", "hbase")
    hadoopConfig2.set("titan.hadoop.input.conf.storage.batch-loading", "false")
    val faunusConf2 = new ModifiableHadoopConfiguration(hadoopConfig2)
    faunusConf2.set(TITAN_INPUT_VERSION, "current")

    val titanHadoopCacheConf2 = new TitanHadoopCacheConfiguration(faunusConf2)

    titanHadoopCacheConf should not equal (titanHadoopCacheConf2)
    titanHadoopCacheConf.hashCode() should not equal (titanHadoopCacheConf2.hashCode())
  }
  "TitanHadoopCacheConfiguration" should "throw an IllegalArgumentException if Faunus configuration is null" in {
    intercept[IllegalArgumentException] {
      val titanHadoopCacheConf = new TitanHadoopCacheConfiguration(null)
    }
  }
}
