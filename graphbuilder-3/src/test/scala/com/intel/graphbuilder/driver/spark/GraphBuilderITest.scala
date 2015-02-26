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

package com.intel.graphbuilder.driver.spark

import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilder, GraphBuilderConfig }
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import com.intel.graphbuilder.parser.rule.{ EdgeRule, VertexRule }
import com.intel.graphbuilder.parser.{ ColumnDef, InputSchema }
import com.intel.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.testutils.{ TestingTitan, TestingSparkContextWordSpec }
import com.tinkerpop.blueprints.Direction
import org.apache.spark.rdd.RDD
import org.scalatest.{ BeforeAndAfter, Matchers }

import scala.collection.JavaConversions._
import com.intel.graphbuilder.driver.spark.rdd.EnvironmentValidator

/**
 * End-to-end Integration Test
 */
class GraphBuilderITest extends TestingSparkContextWordSpec with Matchers with TestingTitan with BeforeAndAfter {

  before {
    EnvironmentValidator.skipEnvironmentValidation = true

    setupTitan()
  }

  after {
    cleanupTitan()
  }

  "GraphBuilder" should {

    "support an end-to-end flow of the numbers example with append" in {

      // Input Data
      val inputRows = List(
        List("1", "{(1)}", "1", "Y", "1", "Y"),
        List("2", "{(1)}", "10", "Y", "2", "Y"),
        List("3", "{(1)}", "11", "Y", "3", "Y"),
        List("4", "{(1),(2)}", "100", "N", "4", "Y"),
        List("5", "{(1)}", "101", "Y", "5", "Y"))

      // Input Schema
      val inputSchema = new InputSchema(List(
        new ColumnDef("cf:number", classOf[String]),
        new ColumnDef("cf:factor", classOf[String]),
        new ColumnDef("binary", classOf[String]),
        new ColumnDef("isPrime", classOf[String]),
        new ColumnDef("reverse", classOf[String]),
        new ColumnDef("isPalindrome", classOf[String])))

      // Parser Configuration
      val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))), VertexRule(gbId("reverse")))
      val edgeRules = List(EdgeRule(gbId("cf:number"), gbId("reverse"), constant("reverseOf")))

      // Setup data in Spark
      val inputRdd = sparkContext.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Connect to the graph
      val titanConfig = new SerializableBaseConfiguration()
      titanConfig.copy(titanBaseConfig)
      val titanConnector = new TitanGraphConnector(titanConfig)

      // Build the Graph
      val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig)
      val gb = new GraphBuilder(config)
      gb.build(inputRdd)

      // Validate
      titanGraph.getEdges.size shouldBe 5
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 5 //Need wrapper due to ambiguous reference errors in Titan 0.5.1+

      // need to shutdown because only one connection can be open at a time
      titanGraph.shutdown()

      // Now we'll append to the existing graph

      // define more input
      val additionalInputRows = List(
        List("5", "{(1)}", "101", "Y", "5", "Y"), // this row overlaps with above
        List("6", "{(1),(2),(3)}", "110", "N", "6", "Y"),
        List("7", "{(1)}", "111", "Y", "7", "Y"))

      val inputRdd2 = sparkContext.parallelize(additionalInputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Append to the existing Graph
      val gb2 = new GraphBuilder(config.copy(append = true, retainDanglingEdges = true, inferSchema = false))
      gb2.build(inputRdd2)

      // Validate
      titanGraph = titanConnector.connect()
      titanGraph.getEdges.size shouldBe 7
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 7

    }
    "support an end-to-end flow of the numbers example with append using broadcast variables" in {

      // Input Data
      val inputRows = List(
        List("1", "{(1)}", "1", "Y", "1", "Y"),
        List("2", "{(1)}", "10", "Y", "2", "Y"),
        List("3", "{(1)}", "11", "Y", "3", "Y"),
        List("4", "{(1),(2)}", "100", "N", "4", "Y"),
        List("5", "{(1)}", "101", "Y", "5", "Y"))

      // Input Schema
      val inputSchema = new InputSchema(List(
        new ColumnDef("cf:number", classOf[String]),
        new ColumnDef("cf:factor", classOf[String]),
        new ColumnDef("binary", classOf[String]),
        new ColumnDef("isPrime", classOf[String]),
        new ColumnDef("reverse", classOf[String]),
        new ColumnDef("isPalindrome", classOf[String])))

      // Parser Configuration
      val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))), VertexRule(gbId("reverse")))
      val edgeRules = List(EdgeRule(gbId("cf:number"), gbId("reverse"), constant("reverseOf")))

      // Setup data in Spark
      val inputRdd = sparkContext.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Connect to the graph
      val titanConfig = new SerializableBaseConfiguration()
      titanConfig.copy(titanBaseConfig)
      val titanConnector = new TitanGraphConnector(titanConfig)

      // Build the Graph
      val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig, broadcastVertexIds = true)
      val gb = new GraphBuilder(config)
      gb.build(inputRdd)

      // Validate
      titanGraph.getEdges.size shouldBe 5
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 5 //Need wrapper due to ambiguous reference errors in Titan 0.5.1+

      // need to shutdown because only one connection can be open at a time
      titanGraph.shutdown()

      // Now we'll append to the existing graph

      // define more input
      val additionalInputRows = List(
        List("5", "{(1)}", "101", "Y", "5", "Y"), // this row overlaps with above
        List("6", "{(1),(2),(3)}", "110", "N", "6", "Y"),
        List("7", "{(1)}", "111", "Y", "7", "Y"))

      val inputRdd2 = sparkContext.parallelize(additionalInputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Append to the existing Graph
      val gb2 = new GraphBuilder(config.copy(append = true, retainDanglingEdges = true, inferSchema = false, broadcastVertexIds = true))
      gb2.build(inputRdd2)

      // Validate
      titanGraph = titanConnector.connect()
      titanGraph.getEdges.size shouldBe 7
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 7

    }

    "support unusual cases of dynamic parsing, dangling edges" in {

      // Input Data
      val inputRows = List(
        List("userId", 1001L, "President Obama", "", "", ""), // a vertex that is a user named President Obama
        List("movieId", 1001L, "When Harry Met Sally", "", "", ""), // a vertex representing a movie
        List("movieId", 1002L, "Frozen", "", "", ""),
        List("movieId", 1003L, "The Hobbit", "", "", ""),
        List("", "", "", 1001L, "likes", 1001L), // edge that means "Obama likes When Harry Met Sally"
        List("userId", 1004L, "Abraham Lincoln", "", "", ""),
        List("", "", "", 1004L, "likes", 1001L), // edge that means "Lincoln likes When Harry Met Sally"
        List("", "", "", 1004L, "likes", 1001L), // duplicate edge that means "Lincoln likes When Harry Met Sally"
        List("", "", "", 1004L, "hated", 1002L), // edge that means "Lincoln hated Frozen"
        List("", "", "", 1001L, "hated", 1003L), // edge that means "Obama hated The Hobbit"
        List("", "", "", 1001L, "hated", 1007L) // edge that means "Obama hated a movie that is a dangling edge"
      )

      // Input Schema
      val inputSchema = new InputSchema(List(
        new ColumnDef("idType", classOf[String]), // describes the next column, if it is a movieId or userId
        new ColumnDef("id", classOf[java.lang.Long]),
        new ColumnDef("name", classOf[String]), // movie title or user name
        new ColumnDef("userIdOfRating", classOf[String]),
        new ColumnDef("liking", classOf[String]),
        new ColumnDef("movieIdOfRating", classOf[String])))

      // Parser Configuration
      val vertexRules = List(VertexRule(column("idType") -> column("id"), List(property("name"))))

      val edgeRules = List(
        EdgeRule(constant("userId") -> column("userIdOfRating"), constant("movieId") -> column("movieIdOfRating"), column("liking"), Nil),
        EdgeRule(constant("movieId") -> column("movieIdOfRating"), constant("userId") -> column("userIdOfRating"), "was-watched-by", Nil))

      // Setup data in Spark
      val inputRdd = sparkContext.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Build the Graph
      val titanConfig = new SerializableBaseConfiguration()
      titanConfig.copy(titanBaseConfig)
      val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig, retainDanglingEdges = true)
      val gb = new GraphBuilder(config)
      gb.build(inputRdd)

      // Validate
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 6 //Need wrapper due to ambiguous reference errors in Titan 0.5.1+
      titanGraph.getEdges.size shouldBe 12

      val obama = titanGraph.getVertices("userId", 1001L).iterator().next()
      obama.getProperty("name").asInstanceOf[String] shouldBe "President Obama"
      obama.getEdges(Direction.OUT).size shouldBe 3
    }

    "support inferring schema from the data" in {

      // Input data, as edge list
      val inputEdges = List(
        "1 2",
        "1 3",
        "1 4",
        "2 1",
        "2 5",
        "3 1",
        "3 4",
        "3 6",
        "3 7",
        "3 8",
        "4 1",
        "4 3",
        "5 2",
        "5 6",
        "5 7",
        "6 3",
        "6 5",
        "7 3",
        "7 5",
        "8 3")

      val inputRows = sparkContext.parallelize(inputEdges)

      // Create edge set RDD, make up properties
      val inputRdd = inputRows.map(row => row.split(" "): Seq[String])
      val edgeRdd = inputRdd.map(e => new GBEdge(None, new Property("userId", e(0)), new Property("userId", e(1)), "tweeted", Set(new Property("tweet", "blah blah blah..."))))

      // Create vertex set RDD, make up properties
      val rawVertexRdd = inputRdd.flatMap(row => row).distinct()
      val vertexRdd = rawVertexRdd.map(v => new GBVertex(new Property("userId", v), Set(new Property("location", "Oregon"))))

      // Build the graph
      val titanConfig = new SerializableBaseConfiguration()
      titanConfig.copy(titanBaseConfig)
      val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig))
      gb.buildGraphWithSpark(vertexRdd, edgeRdd)

      // Validate
      val titanConnector = new TitanGraphConnector(titanConfig)
      titanGraph = titanConnector.connect()

      titanGraph.getEdges.size shouldBe 20
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 8 //Need wrapper due to ambiguous reference errors in Titan 0.5.1+

      val vertexOne = titanGraph.getVertices("userId", "1").iterator().next()
      vertexOne.getProperty("location").asInstanceOf[String] shouldBe "Oregon"
      vertexOne.getEdges(Direction.OUT).size shouldBe 3
      vertexOne.getEdges(Direction.OUT).iterator().next().getProperty("tweet").asInstanceOf[String] shouldBe "blah blah blah..."
    }

  }
}
