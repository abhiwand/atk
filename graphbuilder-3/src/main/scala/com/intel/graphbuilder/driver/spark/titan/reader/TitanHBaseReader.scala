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

package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.driver.spark.rdd.TitanReaderRdd
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader._
import com.intel.graphbuilder.elements.GraphElement
import com.intel.graphbuilder.graph.titan.{ TitanHadoopHBaseCacheListener, TitanAutoPartitioner, TitanGraphConnector }
import com.intel.graphbuilder.titan.io.GBTitanHBaseInputFormat
import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.thinkaurelius.titan.hadoop.formats.titan_050.hbase.CachedTitanHBaseRecordReader
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ SparkListenerApplicationEnd, SparkListener }

import scala.collection.JavaConversions._

/**
 * This is a TitanReader that runs on Spark, and reads a Titan graph from a HBase storage backend.
 *
 * @param sparkContext Spark context
 * @param titanConnector Connector to Titan
 */
class TitanHBaseReader(sparkContext: SparkContext, titanConnector: TitanGraphConnector) extends TitanReader(sparkContext, titanConnector) {
  require(titanConfig.containsKey(TITAN_STORAGE_HOSTNAME), "could not find key " + TITAN_STORAGE_HOSTNAME)
  require(titanConfig.containsKey(TITAN_STORAGE_HBASE_TABLE), "could not find key " + TITAN_STORAGE_HBASE_TABLE)

  /**
   * Read Titan graph from a HBase storage backend into a Spark RDD of graph elements.
   *
   * The RDD returns an iterable of both vertices and edges using GraphBuilder's GraphElement trait. The GraphElement
   * trait is an interface implemented by both vertices and edges.
   *
   * @return RDD of GraphBuilder elements
   */
  override def read(): RDD[GraphElement] = {
    val hBaseConfig = createHBaseConfiguration()
    val tableName = titanConfig.getString(TITAN_STORAGE_HBASE_TABLE)

    checkTableExists(hBaseConfig, tableName)

    val hBaseRDD = sparkContext.newAPIHadoopRDD(hBaseConfig, classOf[GBTitanHBaseInputFormat],
      classOf[NullWritable],
      classOf[FaunusVertex])

    sparkContext.addSparkListener(new TitanHadoopHBaseCacheListener())
    new TitanReaderRdd(hBaseRDD, titanConnector)

  }

  /**
   * Create HBase configuration for connecting to HBase table
   */
  private def createHBaseConfiguration(): org.apache.hadoop.conf.Configuration = {
    val hBaseConfig = HBaseConfiguration.create()

    // Add Titan configuratoin
    titanConfig.getKeys.foreach {
      case (titanKey: String) =>
        val titanHadoopKey = TITAN_HADOOP_PREFIX + titanKey
        hBaseConfig.set(titanHadoopKey, titanConfig.getProperty(titanKey).toString)
    }

    // Auto-configure number of input splits
    val tableName = titanConfig.getString(TITAN_STORAGE_HBASE_TABLE)
    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    titanAutoPartitioner.setSparkHBaseInputSplits(sparkContext, hBaseConfig, tableName)

    hBaseConfig
  }

  /**
   * Throw an exception if the HBase table does not exist.
   *
   * @param hBaseConfig HBase configuration
   * @param tableName HBase table name
   */
  private def checkTableExists(hBaseConfig: org.apache.hadoop.conf.Configuration, tableName: String) = {
    val admin = new HBaseAdmin(hBaseConfig)
    if (!admin.isTableAvailable(tableName)) {
      admin.close()
      throw new RuntimeException("HBase table does not exist: " + tableName + " (graph may not have been loaded with any data)")
    }
    admin.close()
  }
}
