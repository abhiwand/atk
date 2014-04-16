package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.testutils.TestingSparkContext
import com.intel.graphbuilder.parser.{CombinedParser, InputSchema, ColumnDef}
import com.intel.graphbuilder.parser.rule.{EdgeRuleParser, VertexRuleParser, EdgeRule, VertexRule}
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import org.apache.spark.rdd.RDD
import org.specs2.mutable.Specification
import GraphBuilderRDDImplicits._
import org.specs2.mock.Mockito

class ParserRDDFunctionsITest extends Specification with Mockito {

  "ParserRDDFunctions" should {

    "support a combined parser (one that goes over the input in one step)" in new TestingSparkContext {

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

      val vertexParser = new VertexRuleParser(inputSchema, vertexRules)
      val edgeParser = new EdgeRuleParser(inputSchema, edgeRules)
      val parser = new CombinedParser(inputSchema, vertexParser, edgeParser)

      // Setup data in Spark
      val inputRdd = sc.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // invoke method under test
      val outputRdd = inputRdd.parse(parser)

      // verification
      outputRdd.count() mustEqual 15
      outputRdd.filterVertices().count() mustEqual 10
      outputRdd.filterEdges().count() mustEqual 5
    }
  }

}
