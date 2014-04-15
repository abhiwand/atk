package com.intel.graphbuilder.parser.rule

import com.intel.graphbuilder.parser._

/**
 * Marker interface
 */
trait ParseRule {

}

/**
 * A VertexRule describes how to parse Vertices from InputRows
 * @param gbId a PropertyRule that defines a property will be used as the unique property for parsed vertices
 * @param propertyRules rules describing how to parse properties
 */
case class VertexRule(gbId: PropertyRule, propertyRules: List[PropertyRule] = Nil) extends ParseRule {

  /**
   * Does this rule apply to the supplied row
   */
  def appliesTo(row: InputRow): Boolean = {
    gbId appliesTo row
  }

  /**
   * The complete list of property rules including the special gbId rule
   */
  def fullPropertyRules: List[PropertyRule] = {
    gbId :: propertyRules
  }
}


/**
 * An EdgeRule describes how to parse Edges from InputRows
 * @param tailVertexGbId the rule describing how to parse the source Vertex unique Id
 * @param headVertexGbId the rule describing how to parse the destination Vertex unique Id
 * @param label the value of the Edge label
 * @param propertyRules rules describing how to parse properties
 */
case class EdgeRule(tailVertexGbId: PropertyRule, headVertexGbId: PropertyRule, label: Value, propertyRules: List[PropertyRule] = Nil) extends ParseRule {

  /**
   * Does this rule apply to the supplied row
   */
  def appliesTo(row: InputRow): Boolean = {
    headVertexGbId.appliesTo(row) && tailVertexGbId.appliesTo(row) && label.in(row)
  }

}