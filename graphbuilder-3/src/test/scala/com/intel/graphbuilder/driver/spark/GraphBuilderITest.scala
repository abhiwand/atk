package com.intel.graphbuilder.driver.spark

import com.intel.graphbuilder.testutils.{TestingSparkContext, TestingTitan}
import com.intel.graphbuilder.parser.{ColumnDef, InputSchema}
import com.intel.graphbuilder.parser.rule.{EdgeRule, VertexRule}
import org.specs2.mutable.Specification
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import com.intel.graphbuilder.driver.spark.titan.{GraphBuilder, GraphBuilderConfig}
import com.tinkerpop.blueprints.Direction

/**
 * End-to-end Integration Test
 */
class GraphBuilderITest extends Specification {

  "GraphBuilder" should {

    "support an end-to-end flow of the numbers example with append" in new TestingSparkContext with TestingTitan {

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
      val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))), VertexRule(gbId("reverse")))
      val edgeRules = List(EdgeRule(gbId("cf:number"), gbId("reverse"), constant("reverseOf")))

      // Setup data in Spark
      val inputRdd = sc.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Build the Graph
      val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig)
      val gb = new GraphBuilder(config)
      gb.build(inputRdd)

      // Validate
      graph.getEdges.size mustEqual 5
      graph.getVertices.size mustEqual 5

      // need to shutdown because only one connection can be open at a time
      graph.shutdown()

      // Now we'll append to the existing graph

      // define more input
      val additionalInputRows = List(
        List("5", "{(1)}", "101", "Y", "5", "Y"), // this row overlaps with above
        List("6", "{(1),(2),(3)}", "110", "N", "6", "Y"),
        List("7", "{(1)}", "111", "Y", "7", "Y")
      )

      val inputRdd2 = sc.parallelize(additionalInputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Append to the existing Graph
      val gb2 = new GraphBuilder(config.copy(append = true, retainDanglingEdges = true, biDirectional = true, inferSchema = false))
      gb2.build(inputRdd2)

      // Validate
      graph = titanConnector.connect()
      graph.getEdges.size mustEqual 7
      graph.getVertices.size mustEqual 7

    }

    "support unusual cases of dynamic parsing, dangling edges" in new TestingSparkContext with TestingTitan {

      // Input Data
      val inputRows = List(
        List("userId", 1001L,"President Obama", "", "", ""), // a vertex that is a user named President Obama
        List("movieId", 1001L, "When Harry Met Sally", "", "", ""), // a vertex representing a movie
        List("movieId", 1002L, "Frozen", "", "", ""),
        List("movieId", 1003L, "The Hobbit", "", "", ""),
        List("", "", "", 1001L, "likes", 1001L), // edge that means "Obama likes When Harry Met Sally"
        List("userId", 1004L,"Abraham Lincoln", "", "", ""),
        List("", "", "", 1004L, "likes", 1001L), // edge that means "Lincoln likes When Harry Met Sally"
        List("", "", "", 1004L, "likes", 1001L), // duplicate edge that means "Lincoln likes When Harry Met Sally"
        List("", "", "", 1004L, "hated", 1002L), // edge that means "Lincoln hated Frozen"
        List("", "", "", 1001L, "hated", 1003L), // edge that means "Obama hated The Hobbit"
        List("", "", "", 1001L, "hated", 1007L)  // edge that means "Obama hated a movie that is a dangling edge"
      )

      // Input Schema
      val inputSchema = new InputSchema(List(
        new ColumnDef("idType", classOf[String]), // describes the next column, if it is a movieId or userId
        new ColumnDef("id", classOf[java.lang.Long]),
        new ColumnDef("name", classOf[String]), // movie title or user name
        new ColumnDef("userIdOfRating", classOf[String]),
        new ColumnDef("liking", classOf[String]),
        new ColumnDef("movieIdOfRating", classOf[String])
      ))

      // Parser Configuration
      val vertexRules = List(VertexRule(column("idType") -> column("id"), List(property("name"))))

      val edgeRules = List(
        EdgeRule(constant("userId") -> column("userIdOfRating"), constant("movieId") -> column("movieIdOfRating"), column("liking"), Nil),
        EdgeRule(constant("movieId") -> column("movieIdOfRating"), constant("userId") -> column("userIdOfRating"), "was-watched-by", Nil)
      )

      // Setup data in Spark
      val inputRdd = sc.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Build the Graph
      val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig, retainDanglingEdges = true)
      val gb = new GraphBuilder(config)
      gb.build(inputRdd)

      // Validate
      graph.getVertices.size mustEqual 6
      graph.getEdges.size mustEqual 10

      val obama = graph.getVertices("userId", 1001L).iterator().next()
      obama.getProperty("name").asInstanceOf[String] mustEqual "President Obama"
      obama.getEdges(Direction.OUT).size mustEqual 3
    }

  }
}
