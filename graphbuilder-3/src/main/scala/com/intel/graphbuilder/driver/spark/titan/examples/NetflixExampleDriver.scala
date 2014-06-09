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

package com.intel.graphbuilder.driver.spark.titan.examples

import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilder, GraphBuilderConfig }
import com.intel.graphbuilder.parser.ColumnDef
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import com.intel.graphbuilder.parser.rule.{ EdgeRule, VertexRule }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import java.util.Date
import org.apache.spark.{ SparkContext, SparkConf }

/**
 * Example of building a Graph using a Netflix file as input.
 */
object NetflixExampleDriver {

  // Titan Settings
  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.backend", "hbase")
  titanConfig.setProperty("storage.tablename", "netflix")
  //titanConfig.setProperty("storage.backend", "cassandra")
  //titanConfig.setProperty("storage.keyspace", "netflix")
  titanConfig.setProperty("storage.hostname", ExamplesUtils.storageHostname)
  titanConfig.setProperty("storage.batch-loading", "true")
  titanConfig.setProperty("autotype", "none")
  titanConfig.setProperty("storage.buffer-size", "2048")
  titanConfig.setProperty("storage.attempt-wait", "300")
  titanConfig.setProperty("storage.lock-wait-time", "400")
  titanConfig.setProperty("storage.lock-retries", "15")
  titanConfig.setProperty("storage.idauthority-retries", "30")
  titanConfig.setProperty("storage.write-attempts", "10")
  titanConfig.setProperty("storage.read-attempts", "6")
  titanConfig.setProperty("ids.block-size", "300000")
  titanConfig.setProperty("ids.renew-timeout", "150000")

  // Input Schema
  val inputSchema = new InputSchema(List(
    new ColumnDef("userId", classOf[String]),
    new ColumnDef("vertexType", classOf[String]),
    new ColumnDef("movieId", classOf[String]),
    new ColumnDef("rating", classOf[String]),
    new ColumnDef("splits", classOf[String])
  ))

  // Parser Configuration
  val vertexRules = List(VertexRule(property("userId"), List(property("vertexType"))), VertexRule(property("movieId")))
  val edgeRules = List(EdgeRule(property("userId"), property("movieId"), "rates", List(property("rating"), property("splits"))))

  /**
   * This is an example of building a graph on Titan with a Cassandra backend using Spark.
   */
  def main(args: Array[String]) {

    println("start " + new Date())

    // Initialize Spark Connection
    val conf = new SparkConf()
      .setMaster(ExamplesUtils.sparkMaster)
      .setAppName(this.getClass.getSimpleName + " " + new Date())
      .setSparkHome(ExamplesUtils.sparkHome)
      .setJars(List(ExamplesUtils.gbJar))
    conf.set("spark.executor.memory", "32g")
    conf.set("spark.cores.max", "33")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")

    val sc = new SparkContext(conf)

    // Setup data in Spark
    val inputRows = sc.textFile(ExamplesUtils.movieDataset, System.getProperty("PARTITIONS", "100").toInt)
    val inputRdd = inputRows.map(row => row.split(","): Seq[_])

    // Build the Graph
    val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig, biDirectional = false, append = false, broadcastVertexIds = false)
    val gb = new GraphBuilder(config)
    gb.build(inputRdd)

    println("done " + new Date())

  }

}
