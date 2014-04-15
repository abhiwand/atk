package com.intel.graphbuilder.parser.rule

import com.intel.graphbuilder.elements.Property
import com.intel.graphbuilder.parser._


/**
 * Parse zero or more properties using a list of rules
 */
case class PropertyRuleParser(propertyRules: Seq[PropertyRule]) extends Serializable {

  // create a parser for each rule
  val propertyParsers = propertyRules.map(rule => rule -> new SinglePropertyRuleParser(rule)).toMap

  /**
   * Parser zero or more properties from the supplied input using the configured rules.
   */
  def parse(row: InputRow): Seq[Property] = {
    for {
      rule <- propertyRules
      if rule appliesTo row
    } yield propertyParsers(rule).parse(row)
  }

}

/**
 * Always parse a singe property using a single rule.
 *
 * This parser should be used for GbId's.
 */
private[rule] case class SinglePropertyRuleParser(rule: PropertyRule) extends Serializable {

  /**
   * Always parse a singe property from the supplied input using the configured rule.
   */
  def parse(row: InputRow): Property = {
    new Property(rule.key.value(row), rule.value.value(row))
  }
}
