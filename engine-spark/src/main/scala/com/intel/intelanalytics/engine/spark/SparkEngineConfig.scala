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

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.shared.{ EventLogging, SharedConfig }
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import java.net.InetAddress
import java.io.File

/**
 * Configuration Settings for the SparkEngine,
 *
 * This is our wrapper for Typesafe config.
 */
object SparkEngineConfig extends SharedConfig with EventLogging {

  // val's are not lazy because failing early is better

  /** Spark home directory, e.g. "/opt/cloudera/parcels/CDH/lib/spark", "/usr/lib/spark", etc. */
  val sparkHome: String = {
    val sparkHome = config.getString("intel.analytics.engine.spark.home")
    if (sparkHome == "") {
      guessSparkHome
    }
    else {
      sparkHome
    }
  }

  /**
   * Check for sparkHome in the expected locations
   */
  private def guessSparkHome: String = {
    val possibleSparkHomes = List("/usr/lib/spark", "/opt/cloudera/parcels/CDH/lib/spark/")
    possibleSparkHomes.foreach(dir => {
      val path = new File(dir)
      if (path.exists()) {
        return path.getAbsolutePath
      }
    })
    throw new RuntimeException("sparkHome wasn't found at any of the expected locations, please set sparkHome in the config")
  }

  /** URL for spark master, e.g. "spark://hostname:7077", "local[4]", etc */
  val sparkMaster: String = {
    val sparkMaster = config.getString("intel.analytics.engine.spark.master")
    if (sparkMaster == "") {
      "spark://" + hostname + ":7077"
    }
    else {
      sparkMaster
    }
  }

  /** Default number for partitioning data */
  val sparkDefaultPartitions: Int = config.getInt("intel.analytics.engine.spark.default-partitions")

  val defaultTimeout: FiniteDuration = config.getInt("intel.analytics.engine.default-timeout").seconds

  val fsRoot: String = config.getString("intel.analytics.engine.fs.root")

  val maxRows: Int = config.getInt("intel.analytics.engine.max-rows")

  /* number of rows taken for sample test during frame loading */
  val frameLoadTestSampleSize: Int =
    config.getInt("intel.analytics.engine-spark.command.dataframes.load.config.schema-validation-sample-rows")

  /* percentage of maximum rows fail in parsing in sampling test. 50 means up 50% is allowed */
  val frameLoadTestFailThresholdPercentage: Int =
    config.getInt("intel.analytics.engine-spark.command.dataframes.load.config.schema-validation-fail-threshold-percentage")

  /**
   * A list of archives that will be searched for command plugins
   */
  val archives: List[String] = {
    config.getStringList("intel.analytics.engine.plugin.command.archives")
      .asScala
      .toList
  }

  /**
   * Default settings for Titan Load.
   *
   * Creates a new configuration bean each time so it can be modified by the caller (like setting the table name).
   */
  def titanLoadConfiguration: SerializableBaseConfiguration = {
    createTitanConfiguration(config, "intel.analytics.engine.titan.load")
  }

  /**
   * Create new configuration for Titan using properties specified in path expression.
   *
   * This method can also be used by command plugins in the Spark engine which might use
   * a different configuration object.
   *
   * @param commandConfig Configuration object for command.
   * @param titanPath Dot-separated expressions with Titan config, e.g., intel.analytics.engine.titan.load
   * @return Titan configuration
   */
  def createTitanConfiguration(commandConfig: Config, titanPath: String): SerializableBaseConfiguration = {
    val titanConfiguration = new SerializableBaseConfiguration
    val titanDefaultConfig = commandConfig.getConfig(titanPath)
    for (entry <- titanDefaultConfig.entrySet().asScala) {
      titanConfiguration.addProperty(entry.getKey, titanDefaultConfig.getString(entry.getKey))
    }
    titanConfiguration
  }

  /**
   * Configuration properties that will be supplied to SparkConf()
   */
  val sparkConfProperties: Map[String, String] = {
    var sparkConfProperties = Map[String, String]()
    val properties = config.getConfig("intel.analytics.engine.spark.conf.properties")
    for (entry <- properties.entrySet().asScala) {
      sparkConfProperties += entry.getKey -> properties.getString(entry.getKey)
    }
    sparkConfProperties
  }

  /** Hostname for current system */
  private def hostname: String = InetAddress.getLocalHost.getHostName

  // log important settings
  def logSettings(): Unit = withContext("SparkEngineConfig") {
    info("fsRoot: " + fsRoot)
    info("sparkHome: " + sparkHome)
    info("sparkMaster: " + sparkMaster)
    for ((key: String, value: String) <- sparkConfProperties) {
      info(s"sparkConfProperties: $key = $value")
    }
  }

  // Python execution command for workers
  val pythonWorkerExec: String = config.getString("intel.analytics.engine.spark.python-worker-exec")
}
