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

package com.intel.graphbuilder.graph.titan

import com.intel.graphbuilder.io.HBaseTableInputFormat
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.SparkContext

import scala.util.Try

/**
 * Auto-partitioner for Titan graphs that is used to increase concurrency of reads and writes to Titan storage backends.
 *
 * @param titanConfig Titan configuration
 */
case class TitanAutoPartitioner(titanConfig: Configuration) {
  import TitanAutoPartitioner._

  val enableAutoPartition = titanConfig.getBoolean(ENABLE_AUTO_PARTITION, false)

  /**
   * Set the number of HBase pre-splits in the Titan configuration
   *
   * Updates the Titan configuration option for region count, if the auto-partitioner is enabled,
   * and the pre-split count exceeds the minimum region count allowed by Titan.
   *
   * @param hBaseConfig HBase configuration
   */
  def setHBasePreSplits(hBaseConfig: org.apache.hadoop.conf.Configuration): Unit = {
    if (enableAutoPartition) {
      val hBaseAdmin = new HBaseAdmin(hBaseConfig)
      val regionCount = getHBasePreSplits(hBaseAdmin)
      if (regionCount >= HBaseStoreManager.MIN_REGION_COUNT) {
        titanConfig.setProperty(TITAN_HBASE_REGION_COUNT, regionCount)
      }
    }
  }

  /**
   * Sets the HBase input splits for the HBase table input format.
   *
   * @param sparkContext Spark context
   * @param hBaseConfig HBase configuration
   * @param titanGraphName  Titan graph name
   */
  def setHBaseInputSplits(sparkContext: SparkContext,
                          hBaseConfig: org.apache.hadoop.conf.Configuration,
                          titanGraphName: String): Unit = {
    println("About to HBase split regions")
    if (enableAutoPartition) {
      println("Enable splits is true")
      val inputSplits = getSparkHBaseInputSplits(sparkContext, hBaseConfig, titanGraphName)
      println("Trying to create " + inputSplits + " input splits")
      if (inputSplits > 1) {
        hBaseConfig.setInt(HBaseTableInputFormat.NUM_REGION_SPLITS, inputSplits)
        println("Updating HBase configuration to " + inputSplits + " input splits")
        println("Checking if config was set to  " + hBaseConfig.getInt(HBaseTableInputFormat.NUM_REGION_SPLITS, -1))
      }
    }
  }

  /**
   * Get input splits for Titan/HBase reader for Spark.
   *
   * The default input split policy for HBase tables is one Spark partition per HBase region. This
   * function computes the desired number of HBase input splits based on the number of available Spark
   * cores in the cluster, and the user-defined configuration for splits/core.
   *
   * Number of input splits = max(Number of HBase regions for table, (available Spark cores*input splits/core))
   *
   * @param sparkContext Spark context
   * @param hBaseConfig HBase configuration
   * @param titanGraphName Titan graph name
   * @return Desired number of HBase input splits
   */
  private def getSparkHBaseInputSplits(sparkContext: SparkContext,
                                       hBaseConfig: org.apache.hadoop.conf.Configuration,
                                       titanGraphName: String): Int = {
    val hBaseAdmin = new HBaseAdmin(hBaseConfig)
    val regionCount = Try(hBaseAdmin.getTableRegions(TableName.valueOf(titanGraphName)).size()).getOrElse(0)

    val maxSparkCores = getMaxSparkCores(sparkContext)
    val splitsPerCore = titanConfig.getInt(HBASE_INPUT_SPLITS_PER_CORE, 1)
    println("Max spark cores: " + maxSparkCores)
    println("Splits per core: " + splitsPerCore)
    println("Region count: " + regionCount)
    Math.max(splitsPerCore * maxSparkCores, regionCount)
  }

  /**
   * Get HBase pre-splits
   *
   * Uses a simple rule-of-thumb to calculate the number of regions per table. This is a place-holder
   * while we figure out how to incorporate other parameters such as input-size, and number of HBase column
   * families into the formula.
   *
   * @param hBaseAdmin HBase administration
   * @return Number of HBase pre-splits
   */
  private def getHBasePreSplits(hBaseAdmin: HBaseAdmin): Int = {
    val regionsPerServer = titanConfig.getInt(HBASE_REGIONS_PER_SERVER, 1)
    val regionServerCount = getHBaseRegionServerCount(hBaseAdmin)

    val preSplits = if (regionServerCount > 0) regionsPerServer * regionServerCount else 0

    preSplits
  }

  /**
   * Get the maximum number of cores available to the Spark cluster
   *
   * @param sparkContext Spark context
   * @return Available Spark cores
   */
  private def getMaxSparkCores(sparkContext: SparkContext): Int = {
    val configuredMaxCores = sparkContext.getConf.getInt(SPARK_MAX_CORES, 0)
    println("Configured spark cores: " + configuredMaxCores)

    val maxSparkCores = if (configuredMaxCores > 0) {
      configuredMaxCores
    }
    else {
      val numCoresPerWorker = Runtime.getRuntime.availableProcessors()
      //val numWorkers = sparkContext.getExecutorStorageStatus.size -- not working well
      val hBaseAdmin = new HBaseAdmin(HBaseConfiguration.create)
      val numWorkers = getHBaseRegionServerCount(hBaseAdmin)
      println("Num cores per worker: " + numCoresPerWorker)
      println("Num workers: " + numWorkers)
      println("Storage stats" + sparkContext.getExecutorStorageStatus)
      (numCoresPerWorker * numWorkers)
    }
    Math.max(0, maxSparkCores)
  }

  /**
   * Get the number of region servers in the HBase cluster.
   *
   * @param hBaseAdmin HBase administration
   * @return Number of region servers
   */
  def getHBaseRegionServerCount(hBaseAdmin: HBaseAdmin): Int = Try({
    hBaseAdmin.getClusterStatus().getServers().size()
  }).getOrElse(-1);

}

object TitanAutoPartitioner {
  val ENABLE_AUTO_PARTITION = "auto-partitioner.enable"
  val HBASE_REGIONS_PER_SERVER = "auto-partitioner.hbase.regions-per-server"
  val HBASE_INPUT_SPLITS_PER_CORE = "auto-partitioner.hbase.input-splits-per-spark-core"

  val SPARK_MAX_CORES = "spark.cores.max"
  val TITAN_HBASE_REGION_COUNT = "storage.region-count" //TODO: Update for 0.5
}