package com.intel.graphbuilder.parser.rule

import com.intel.graphbuilder.elements.Vertex
import com.intel.graphbuilder.parser.{InputSchema, Parser, InputRow}


/**
 * Parse an InputRow into Vertices using VertexRules
 */
case class VertexRuleParser(inputSchema: InputSchema, vertexRules: List[VertexRule]) extends Parser[Vertex](inputSchema) with Serializable {

  // each rule gets its own parser
  private val vertexParsers = vertexRules.map(rule => rule -> new SingleVertexRuleParser(rule)).toMap

  /**
   * Parse the supplied InputRow into zero or more Vertices using all applicable rules
   */
  def parse(row: InputRow): Seq[Vertex] = {
    for {
      rule <- vertexRules
      if rule appliesTo row
    } yield vertexParsers(rule).parse(row)
  }

}

/**
 * Parse an InputRow into a Vertex using a VertexRule
 */
private[rule] case class SingleVertexRuleParser(rule: VertexRule) extends Serializable {

  private val gbIdParser = new SinglePropertyRuleParser(rule.gbId)
  private val propertyParser = new PropertyRuleParser(rule.propertyRules)

  def parse(row: InputRow): Vertex = {
    new Vertex(gbIdParser.parse(row), propertyParser.parse(row))
  }
}


