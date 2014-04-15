package com.intel.graphbuilder.driver.spark

import com.intel.graphbuilder.testutils.{TestingSparkContextWithTitan, TestingSparkContext, TestingTitan}
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.parser.{ColumnDef, InputSchema}
import com.intel.graphbuilder.parser.rule.{EdgeRule, VertexRule}
import org.specs2.mutable.Specification
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import java.util.Date
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import com.intel.graphbuilder.driver.spark.titan.{GraphBuilder, GraphBuilderConfig}

/**
 * End-to-end Integration Test
 */
class GraphBuilderITest extends Specification {

  "GraphBuilder" should {

    "support an end-to-end flow" in new TestingSparkContextWithTitan {

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
    }
  }
}
