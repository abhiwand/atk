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

import com.google.common.annotations.VisibleForTesting
import com.intel.graphbuilder.io.GBTitanHBaseInputFormat
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.conf
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.spark.SparkContext

import scala.util.Try

/**
 * Auto-partitioner for Titan graphs that is used to increase concurrency of reads and writes to Titan storage backends.
 *
 * @param titanConfig Titan configuration
 */
case class TitanAutoPartitioner(titanConfig: Configuration) {

  import com.intel.graphbuilder.graph.titan.TitanAutoPartitioner._

  val enableAutoPartition = titanConfig.getBoolean(ENABLE_AUTO_PARTITION, false)

  /**
   * Set the number of HBase pre-splits in the Titan configuration
   *
   * Updates the Titan configuration option for region count, if the auto-partitioner is enabled,
   * and the pre-split count exceeds the minimum region count allowed by Titan.
   *
   * @param hBaseAdmin HBase administration
   * @return Updated Titan configuration
   */
  def setHBasePreSplits(hBaseAdmin: HBaseAdmin): Configuration = {
    if (enableAutoPartition) {
      val regionCount = getHBasePreSplits(hBaseAdmin)
      if (regionCount >= HBaseStoreManager.MIN_REGION_COUNT) {
        titanConfig.setProperty(TITAN_HBASE_REGION_COUNT, regionCount)
      }
    }
    titanConfig
  }

  /**
   * Sets the HBase input splits for the HBase table input format.
   *
   * @param sparkContext Spark context
   * @param hBaseAdmin HBase administration
   * @param titanGraphName  Titan graph name
   *
   * @return Updated HBase configuration
   */
  def setHBaseInputSplits(sparkContext: SparkContext,
                          hBaseAdmin: HBaseAdmin,
                          titanGraphName: String): org.apache.hadoop.conf.Configuration = {
    val hBaseConfig = hBaseAdmin.getConfiguration
    setHBaseInputSplits(sparkContext, hBaseConfig, titanGraphName)
    hBaseConfig
  }

  /**
   * Sets the HBase input splits for the HBase table input format in the HBase configuration
   * that is supplied as input.
   *
   * @param sparkContext Spark context
   * @param hBaseConfig HBase configuration
   * @param titanGraphName  Titan graph name
   */
  def setHBaseInputSplits(sparkContext: SparkContext,
                          hBaseConfig: org.apache.hadoop.conf.Configuration,
                          titanGraphName: String): Unit = {
    if (enableAutoPartition) {
      val inputSplits = getSparkHBaseInputSplits(sparkContext, new HBaseAdmin(hBaseConfig), titanGraphName)
      if (inputSplits > 1) {
        hBaseConfig.setInt(GBTitanHBaseInputFormat.NUM_REGION_SPLITS, inputSplits)
      }
    }
  }

  /**
   * Get input splits for Titan/HBase reader for Spark.
   *
   * The default input split policy for HBase tables is one Spark partition per HBase region. This
   * function computes the desired number of HBase input splits based on the number of available Spark
   * cores in the cluster, the user-defined configuration for splits/core, and the minimum size of an
   * input split
   *
   * Number of input splits = min((HBase Table size MB/minimum split size MB), (available Spark cores*input splits/core))
   *
   * @param sparkContext Spark context
   * @param hBaseAdmin HBase administration
   * @param titanGraphName Titan graph name
   * @return Desired number of HBase input splits
   */
  private def getSparkHBaseInputSplits(sparkContext: SparkContext,
                                       hBaseAdmin: HBaseAdmin,
                                       titanGraphName: String): Int = {
    val maxSplitsBySize = getMaxInputSplitsBySize(hBaseAdmin, titanGraphName)
    val maxSparkCores = getMaxSparkCores(sparkContext, hBaseAdmin)
    val splitsPerCore = titanConfig.getInt(HBASE_INPUT_SPLITS_PER_CORE, 1)
    Math.min(splitsPerCore * maxSparkCores, maxSplitsBySize)
  }

  /**
   * Get the maximum number of input splits based on the graph size in HBase, and the minimum size of an input split
   *
   * @param hBaseAdmin HBase configuration
   * @param titanGraphName Titan graph name
   * @return Maximum number of input splits
   *
   */
  @VisibleForTesting
  def getMaxInputSplitsBySize(hBaseAdmin: HBaseAdmin, titanGraphName: String): Int = {
    val minSplitSizeMb = titanConfig.getInt(HBASE_MIN_INPUT_SPLIT_SIZE_MB, 0)
    val hBaseConfig = hBaseAdmin.getConfiguration
    val tableName = TableName.valueOf(titanGraphName)

    val regionCount = Try(hBaseAdmin.getTableRegions(tableName).size()).getOrElse(0)
    val tableSizeMb = getTableSizeInMb(hBaseConfig, tableName)

    val maxSplitsBySize = if (minSplitSizeMb > 0) {
      Math.max(tableSizeMb / minSplitSizeMb, regionCount)
    }
    else regionCount

    maxSplitsBySize
  }

  /**
   * Get the size of a HBase table on disk
   *
   * @param hBaseConfig HBase configuration
   * @param tableName HBase table name
   * @return Size of HBase table on disk
   */
  @VisibleForTesting
  def getTableSizeInMb(hBaseConfig: conf.Configuration, tableName: TableName): Int = {
    val tableSize = Try({
      val tableDir = FSUtils.getTableDir(FSUtils.getRootDir(hBaseConfig), tableName)
      val fileSystem = FileSystem.get(hBaseConfig)
      (fileSystem.getContentSummary(tableDir).getLength / (1024 * 1024))
    }).getOrElse(0L)

    if (tableSize > Integer.MAX_VALUE) Integer.MAX_VALUE else tableSize.toInt
  }

  /**
   * Get the maximum number of cores available to the Spark cluster
   *
   * @param sparkContext Spark context
   * @param hBaseAdmin HBase administration
   * @return Available Spark cores
   */
  private def getMaxSparkCores(sparkContext: SparkContext, hBaseAdmin: HBaseAdmin): Int = {
    val configuredMaxCores = sparkContext.getConf.getInt(SPARK_MAX_CORES, 0)

    val maxSparkCores = if (configuredMaxCores > 0) {
      configuredMaxCores
    }
    else {
      // val numWorkers = sparkContext.getExecutorStorageStatus.size -1
      // getExecutorStorageStatus not working correctly and sometimes returns 1 instead of the correct number of workers
      // Using number of region servers to estimate the number of slaves since it is more reliable
      val numWorkers = getHBaseRegionServerCount(hBaseAdmin)
      val numCoresPerWorker = Runtime.getRuntime.availableProcessors()
      (numCoresPerWorker * numWorkers)
    }
    Math.max(0, maxSparkCores)
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
   * Get the number of region servers in the HBase cluster.
   *
   * @param hBaseAdmin HBase administration
   * @return Number of region servers
   */
  private def getHBaseRegionServerCount(hBaseAdmin: HBaseAdmin): Int = Try({
    hBaseAdmin.getClusterStatus() getServersSize
  }).getOrElse(-1);

}

/**
 * Constants used by Titan Auto-partitioner
 */
object TitanAutoPartitioner {
  val ENABLE_AUTO_PARTITION = "auto-partitioner.enable"
  val HBASE_REGIONS_PER_SERVER = "auto-partitioner.hbase.regions-per-server"
  val HBASE_INPUT_SPLITS_PER_CORE = "auto-partitioner.hbase.input-splits-per-spark-core"
  val HBASE_MIN_INPUT_SPLIT_SIZE_MB = "auto-partitioner.hbase.input-splits-per-spark-core"
  val SPARK_MAX_CORES = "spark.cores.max"
  val TITAN_HBASE_REGION_COUNT = "storage.region-count"
}