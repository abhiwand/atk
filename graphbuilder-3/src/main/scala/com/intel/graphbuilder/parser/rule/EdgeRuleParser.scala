package com.intel.graphbuilder.parser.rule

import com.intel.graphbuilder.elements.Edge
import com.intel.graphbuilder.parser._


/**
 * Parse InputRow's into Edges using EdgeRules
 */
case class EdgeRuleParser(inputSchema: InputSchema, edgeRules: List[EdgeRule]) extends Parser[Edge](inputSchema) with Serializable {

  // each rule gets its own parser
  private val edgeParsers = edgeRules.map(rule => rule -> new SingleEdgeRuleParser(rule)).toMap

  /**
   * Parse the supplied InputRow into zero or more Edges using all applicable rules
   */
  def parse(row: InputRow): Seq[Edge] = {
    for {
      rule <- edgeRules
      if rule appliesTo row
    } yield edgeParsers(rule).parse(row)
  }
}


/**
 * Parse a single InputRow into an Edge
 */
private[rule] case class SingleEdgeRuleParser(rule: EdgeRule) extends Serializable {

  // each rule can have different rules for parsing tailVertexGbId's, headVertexGbId's, and properties
  private val tailGbIdParser = new SinglePropertyRuleParser(rule.tailVertexGbId)
  private val headGbIdParser = new SinglePropertyRuleParser(rule.headVertexGbId)
  private val propertyParser = new PropertyRuleParser(rule.propertyRules)

  def parse(row: InputRow): Edge = {
    new Edge(tailGbIdParser.parse(row), headGbIdParser.parse(row), rule.label.value(row), propertyParser.parse(row))
  }
}


