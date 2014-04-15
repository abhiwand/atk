package com.intel.graphbuilder.parser

import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import com.intel.graphbuilder.parser.rule._
import org.specs2.mutable.Specification


class CombinedParserSpec extends Specification {

  "CompinedParser" should {

    "in the numbers example, parse 15 graph elements" in {

      val inputRows = List(
        List("1", "{(1)}", "1", "Y", "1", "Y"),
        List("2", "{(1)}", "10", "Y", "2", "Y"),
        List("3", "{(1)}", "11", "Y", "3", "Y"),
        List("4", "{(1),(2)}", "100", "N", "4", "Y"),
        List("5", "{(1)}", "101", "Y", "5", "Y")
      )

      val parser = {
        val inputSchema = new InputSchema(List("cf:number", "cf:factor", "binary", "isPrime", "reverse", "isPalindrome"), null)
        val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))), VertexRule(gbId("reverse")))
        val edgeRules = List(EdgeRule(property("cf:number"), property("reverse"), constant("reverseOf")))
        new CombinedParser(inputSchema, new VertexRuleParser(inputSchema, vertexRules), new EdgeRuleParser(inputSchema, edgeRules))
      }

      inputRows.flatMap(row => parser.parse(row)).size mustEqual 15
    }

  }
}
