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

import com.intel.graphbuilder.driver.spark.titan.{GraphBuilderConfig, GraphBuilder}
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser.ColumnDef
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import com.intel.graphbuilder.parser.rule._
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import java.util.{Calendar, Date}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.util

/**
 * This is an example of building a graph on Titan with a Cassandra backend using Spark.
 *
 * This example uses RuleParsers
 */
object SparkTitanCassandraExampleDriver {

  // Titan Settings
  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.backend", "cassandra")
  titanConfig.setProperty("storage.hostname", "127.0.0.1")
  titanConfig.setProperty("storage.keyspace", "titan" + System.currentTimeMillis())

  // Input Data
  val inputRows = List(
    List("1", "{(1)}", "1", "Y", "1", "Y"),
    List("2", "{(1)}", "10", "Y", "2", "Y"),
    List("3", "{(1)}", "11", "Y", "3", "Y"),
    List("4", "{(1),(2)}", "100", "N", "4", "Y"),
    List("5", "{(1)}", "101", "Y", "5", "Y")
  )

  // Input Schema
  val inputSchema = new InputSchema(List(
    new ColumnDef("cf:number", classOf[String]),
    new ColumnDef("cf:factor", classOf[String]),
    new ColumnDef("binary", classOf[String]),
    new ColumnDef("isPrime", classOf[String]),
    new ColumnDef("reverse", classOf[String]),
    new ColumnDef("isPalindrome", classOf[String])
  ))

  // Parser Configuration
  val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))),
    VertexRule(gbId("reverse")))
  val edgeRules = List(EdgeRule(gbId("cf:number"), gbId("reverse"), constant("reverseOf")))


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
    val sc = new SparkContext(conf)

    // Setup data in Spark
    val inputRdd = sc.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

    // Build the Graph
    val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig, biDirectional = false, append = false)
    val gb = new GraphBuilder(config)
    gb.build(inputRdd)

    // Print the Graph
    val titanConnector = new TitanGraphConnector(titanConfig)
    val graph = titanConnector.connect()
    try {
      println(ExamplesUtils.dumpGraph(graph))
    }
    finally {
      graph.shutdown()
    }

    println("done " + new Date())

  }

}


