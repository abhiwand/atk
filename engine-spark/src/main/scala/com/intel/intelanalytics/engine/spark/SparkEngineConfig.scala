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

package com.intel.intelanalytics.engine.spark

import java.io.File
import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.intel.event.{ EventContext, EventLogging }
import com.intel.graphbuilder.graph.titan.TitanAutoPartitioner
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

/**
 * Configuration Settings for the SparkEngine,
 *
 * This is our wrapper for Typesafe config.
 */
object SparkEngineConfig extends SparkEngineConfig

/**
 * Configuration Settings for the SparkEngine,
 *
 * This is our wrapper for Typesafe config.
 */
trait SparkEngineConfig extends EventLogging {
  implicit val eventContext: EventContext = null

  val config = ConfigFactory.load()

  // val's are not lazy because failing early is better

  /** Spark home directory, e.g. "/opt/cloudera/parcels/CDH/lib/spark", "/usr/lib/spark", etc. */
  val sparkHome: String = {
    val sparkHome = config.getString("intel.analytics.engine.spark.home")
    if (sparkHome == "") {
      info("Spark Home is NOT configured so guessing where it is")
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
    val possibleSparkHomes = List("/opt/cloudera/parcels/CDH/lib/spark/", "/usr/lib/spark")
    possibleSparkHomes.foreach(dir => {
      val path = new File(dir)
      if (path.exists()) {
        info("Using Spark Home found at " + path.getAbsolutePath)
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

  val isLocalMaster: Boolean = {
    (sparkMaster.startsWith("local[") && sparkMaster.endsWith("]")) || sparkMaster.equals("local")
  }

  /**
   * true to re-use a SparkContext, this can be helpful for automated integration tests, not for customers.
   * NOTE: true should break the progress bar.
   */
  val reuseSparkContext: Boolean = config.getBoolean("intel.analytics.engine.spark.reuse-context")

  val defaultTimeout: FiniteDuration = config.getInt("intel.analytics.engine.default-timeout").seconds

  val fsRoot: String = config.getString("intel.analytics.engine.fs.root")

  val pageSize: Int = config.getInt("intel.analytics.engine.page-size")

  /* number of rows taken for sample test during frame loading */
  val frameLoadTestSampleSize: Int =
    config.getInt("intel.analytics.engine-spark.command.frames.load.config.schema-validation-sample-rows")

  /* percentage of maximum rows fail in parsing in sampling test. 50 means up 50% is allowed */
  val frameLoadTestFailThresholdPercentage: Int =
    config.getInt("intel.analytics.engine-spark.command.frames.load.config.schema-validation-fail-threshold-percentage")

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

    //Prevents errors in Titan/HBase reader when storage.hostname is converted to list
    titanConfiguration.setDelimiterParsingDisabled(true)
    for (entry <- titanDefaultConfig.entrySet().asScala) {
      titanConfiguration.addProperty(entry.getKey, titanDefaultConfig.getString(entry.getKey))
    }

    setTitanAutoPartitions(titanConfiguration)
  }

  /**
   * Update Titan configuration with auto-generated settings.
   *
   * At present, auto-partitioner for graph construction only sets HBase pre-splits.
   *
   * @param titanConfiguration
   * @return Updated Titan configuration
   */
  def setTitanAutoPartitions(titanConfiguration: SerializableBaseConfiguration): SerializableBaseConfiguration = {
    val titanAutoPartitioner = TitanAutoPartitioner(titanConfiguration)
    val storageBackend = titanConfiguration.getString("storage.backend")

    storageBackend.toLowerCase match {
      case "hbase" => {
        val hBaseAdmin = new HBaseAdmin(HBaseConfiguration.create())
        titanAutoPartitioner.setHBasePreSplits(hBaseAdmin)
        info("Setting Titan/HBase pre-splits for  to: " + titanConfiguration.getProperty(TitanAutoPartitioner.TITAN_HBASE_REGION_COUNT))
      }
      case _ => info("No auto-configuration settings for storage backend: " + storageBackend)
    }

    titanConfiguration.setProperty("auto-partitioner.broadcast-join-threshold", broadcastJoinThreshold)

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

  /**
   * Max partitions if file is larger than limit specified in autoPartitionConfig
   */
  val maxPartitions: Int = {
    config.getInt("intel.analytics.engine-spark.auto-partitioner.max-partitions")
  }

  /**
   * Disable all kryo registration in plugins (this is mainly here for performance testing
   * and debugging when someone suspects Kryo might be causing some kind of issue).
   */
  val disableKryo: Boolean = config.getBoolean("intel.analytics.engine.spark.disable-kryo")

  /**
   * Sorted list of mappings for file size to partition size (larger file sizes first)
   */
  val autoPartitionerConfig: List[FileSizeToPartitionSize] = {
    import scala.collection.JavaConverters._
    val key = "intel.analytics.engine-spark.auto-partitioner.file-size-to-partition-size"
    val configs = config.getConfigList(key).asScala.toList
    val unsorted = configs.map(config => {
      val partitions = config.getInt("partitions")
      if (partitions > maxPartitions) {
        throw new RuntimeException("Invalid value partitions:" + partitions +
          " shouldn't be larger than max-partitions:" + maxPartitions + ", under:" + key)
      }
      FileSizeToPartitionSize(config.getBytes("upper-bound"), partitions)
    })
    unsorted.sortWith((leftConfig, rightConfig) => leftConfig.fileSizeUpperBound > rightConfig.fileSizeUpperBound)
  }

  /**
   * Use broadcast join if file size is lower than threshold.
   *
   * A threshold of zero disables broadcast joins. This threshold should not exceed the maximum size of results
   * that can be returned to the Spark driver (i.e., spark.driver.maxResultSize).
   */
  val broadcastJoinThreshold = {
    val joinThreshold = config.getBytes("intel.analytics.engine-spark.auto-partitioner.broadcast-join-threshold")
    val maxResultSize = config.getBytes("intel.analytics.engine.spark.conf.properties.spark.driver.maxResultSize")
    if (joinThreshold > maxResultSize) {
      throw new RuntimeException(
        s"Broadcast join threshold: ${joinThreshold} shouldn't be larger than spark.driver.maxResultSize: ${maxResultSize}")
    }
    joinThreshold
  }

  /**
   * spark driver max size should be minimum of 128M for Spark Submit to work. We are currently loading multiple
   * class loaders and Spark Submit driver throws OOM if default value of 64M is kept for PermGen space
   */
  val sparkDriverMaxPermSize = config.getString("intel.analytics.engine.spark.conf.properties.spark.driver.maxPermSize")

  /**
   * Determines whether SparkContex.addJars() paths get "local:" prefix or not.
   *
   * True if engine-spark.jar, graphon.jar and ohters are installed locally on each cluster node (preferred).
   * False is useful mainly for development on a cluster.  False results in many copies of the application jars
   * being made and copied to all of the cluster nodes.
   */
  val sparkAppJarsLocal: Boolean = config.getBoolean("intel.analytics.engine.spark.app-jars-local")

  /** Fully qualified Hostname for current system */
  private def hostname: String = InetAddress.getLocalHost.getCanonicalHostName

  // log important settings
  def logSettings(): Unit = withContext("SparkEngineConfig") {
    info("fsRoot: " + fsRoot)
    info("sparkHome: " + sparkHome)
    info("sparkMaster: " + sparkMaster)
    info("disableKryo: " + disableKryo)
    for ((key: String, value: String) <- sparkConfProperties) {
      info(s"sparkConfProperties: $key = $value")
    }
    if (sparkAppJarsLocal) {
      info("sparkAppJarsLocal: " + sparkAppJarsLocal + " (expecting application jars to be installed on all worker nodes)")
    }
    else {
      info("sparkAppJarsLocal: " + sparkAppJarsLocal + " (application jars will be copied to worker nodes with every command)")
    }
  }

  // Python execution command for workers
  val pythonWorkerExec: String = config.getString("intel.analytics.engine.spark.python-worker-exec")
  val pythonUdfDependenciesDirectory: String = config.getString("intel.analytics.engine.spark.python-udf-deps-directory")

  // val's are not lazy because failing early is better
  val metaStoreConnectionUrl: String = nonEmptyString("intel.analytics.metastore.connection.url")
  val metaStoreConnectionDriver: String = nonEmptyString("intel.analytics.metastore.connection.driver")
  val metaStoreConnectionUsername: String = config.getString("intel.analytics.metastore.connection.username")
  val metaStoreConnectionPassword: String = config.getString("intel.analytics.metastore.connection.password")

  /**
   * Get a String but throw Exception if it is empty
   */
  protected def nonEmptyString(key: String): String = {
    config.getString(key) match {
      case "" => throw new IllegalArgumentException(key + " cannot be empty!")
      case s: String => s
    }
  }

  //gc variables
  val gcInterval = config.getDuration("intel.analytics.engine.gc.interval", TimeUnit.MILLISECONDS)
  val gcAgeToDeleteData = config.getDuration("intel.analytics.engine.gc.data-lifespan", TimeUnit.MILLISECONDS)
  val gcAgeToDeleteMetaData = config.getDuration("intel.analytics.engine.gc.metadata-lifespan", TimeUnit.MILLISECONDS) + gcAgeToDeleteData

  val enableKerberos: Boolean = config.getBoolean("intel.analytics.engine.hadoop.kerberos.enabled")
  val kerberosPrincipalName: Option[String] = if (enableKerberos) Some(nonEmptyString("intel.analytics.engine.hadoop.kerberos.principal-name")) else None
  val kerberosKeyTabPath: Option[String] = if (enableKerberos) Some(nonEmptyString("intel.analytics.engine.hadoop.kerberos.keytab-file")) else None
}
