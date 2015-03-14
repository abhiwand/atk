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
import com.intel.graphbuilder.graph.titan.{ TitanHadoopCassandraCacheListener, TitanGraphConnector }
import com.thinkaurelius.titan.diskstorage.Backend
import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.thinkaurelius.titan.hadoop.formats.titan_050.cassandra.CachedTitanCassandraInputFormat
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.thrift.{ SlicePredicate, SliceRange }
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
 * This is a TitanReader that runs on Spark, and reads a Titan graph from a Cassandra storage backend.
 *
 * @param sparkContext Spark context
 * @param titanConnector Connector to Titan
 */
class TitanCassandraReader(sparkContext: SparkContext, titanConnector: TitanGraphConnector) extends TitanReader(sparkContext, titanConnector) {
  require(titanConfig.containsKey(TITAN_STORAGE_HOSTNAME), "could not find key " + TITAN_STORAGE_HOSTNAME)
  require(titanConfig.containsKey(TITAN_STORAGE_CASSANDRA_KEYSPACE), "could not find key " + TITAN_STORAGE_CASSANDRA_KEYSPACE)

  /**
   * Read Titan graph from a Cassandra storage backend into a Spark RDD of graph elements.
   *
   * The RDD returns an iterable of both vertices and edges using GraphBuilder's GraphElement trait. The GraphElement
   * trait is an interface implemented by both vertices and edges.
   *
   * @return RDD of GraphBuilder elements
   */
  override def read(): RDD[GraphElement] = {
    val cassandraConfig = createCassandraConfiguration()

    val cassandraRDD = sparkContext.newAPIHadoopRDD(cassandraConfig, classOf[CachedTitanCassandraInputFormat],
      classOf[NullWritable],
      classOf[FaunusVertex])

    sparkContext.addSparkListener(new TitanHadoopCassandraCacheListener())
    new TitanReaderRdd(cassandraRDD, titanConnector)
  }

  /**
   * Create Cassandra configuration for connecting to Cassandra table
   */
  private def createCassandraConfiguration(): org.apache.hadoop.conf.Configuration = {
    val cassandraConfig = sparkContext.hadoopConfiguration //new org.apache.hadoop.conf.Configuration()
    val tableName = titanConfig.getString(TITAN_STORAGE_CASSANDRA_KEYSPACE)
    val port = titanConfig.getString(TITAN_STORAGE_PORT)
    val hostnames = titanConfig.getString(TITAN_STORAGE_HOSTNAME)
    val wideRows = titanConfig.getBoolean(TITAN_CASSANDRA_INPUT_WIDEROWS, false)
    val predicate: SlicePredicate = getSlicePredicate

    ConfigHelper.setInputSlicePredicate(cassandraConfig, predicate)
    ConfigHelper.setInputPartitioner(cassandraConfig, "Murmur3Partitioner")
    ConfigHelper.setOutputPartitioner(cassandraConfig, "Murmur3Partitioner")
    ConfigHelper.setInputRpcPort(cassandraConfig, port)
    ConfigHelper.setInputColumnFamily(cassandraConfig, tableName, Backend.EDGESTORE_NAME, wideRows)

    titanConfig.getKeys.foreach {
      case (titanKey: String) =>
        val titanHadoopKey = TITAN_HADOOP_PREFIX + titanKey
        cassandraConfig.set(titanHadoopKey, titanConfig.getProperty(titanKey).toString)
    }

    cassandraConfig
  }

  /**
   * Get Range of rows (Slice predicate) for Cassandra input format
   * @return Slice predicate
   */
  private def getSlicePredicate: SlicePredicate = {
    val predicate = new SlicePredicate()
    val sliceRange = new SliceRange()
    sliceRange.setStart(Array.empty[Byte])
    sliceRange.setFinish(Array.empty[Byte])
    predicate.setSlice_range(sliceRange)
  }
}
