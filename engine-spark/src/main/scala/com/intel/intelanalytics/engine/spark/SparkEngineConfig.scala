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

package com.intel.intelanalytics.engine.spark

import scala.concurrent.duration._
import com.intel.intelanalytics.shared.SharedConfig
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import scala.collection.JavaConverters._

/**
 * Configuration Settings for the SparkEngine,
 *
 * This is our wrapper for Typesafe config.
 */
object SparkEngineConfig extends SharedConfig {

  /** Spark home directory, e.g. "/opt/cloudera/parcels/CDH/lib/spark", "/usr/lib/spark", etc. */
  val sparkHome: String = config.getString("intel.analytics.spark.home")

  /** URL for spark master, e.g. "spark://hostname:7077", "local[4]", etc */
  val sparkMaster: String = config.getString("intel.analytics.spark.master")

  val defaultTimeout: FiniteDuration = config.getInt("intel.analytics.engine.defaultTimeout").seconds

  val fsRoot: String = config.getString("intel.analytics.fs.root")

  val maxRows: Int = config.getInt("intel.analytics.engine.max-rows")

  /**
   * A list of archives that will be searched for command plugins
   */
  val archives: List[(String, String)] = {
    val cfg = config.getConfig("intel.analytics.engine.archives")
    cfg.entrySet().asScala
        .map(e => (e.getKey, e.getValue.unwrapped().asInstanceOf[String]))
        .toList
  }

  /**
   * Default settings for Titan Load.
   *
   * Creates a new configuration bean each time so it can be modified by the caller (like setting the table name).
   */
  def titanLoadConfiguration: SerializableBaseConfiguration = {
    val titanConfiguration = new SerializableBaseConfiguration
    val titanLoadConfig = config.getConfig("intel.analytics.engine.titan.load")
    for (entry <- titanLoadConfig.entrySet().asScala) {
      titanConfiguration.addProperty(entry.getKey, titanLoadConfig.getString(entry.getKey))
    }
    titanConfiguration
  }

}
