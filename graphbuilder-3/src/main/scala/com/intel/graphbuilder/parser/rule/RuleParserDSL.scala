package com.intel.graphbuilder.parser.rule


/**
 * Methods and implicit conversions that help the creation of ParserRules to be simpler and use a more DSL-like syntax
 */
object RuleParserDSL {

  /**
   * Convert Any to a ConstantValue
   */
  implicit def any2ConstantValue(value: Any) = {
    new ConstantValue(value)
  }

  /**
   * Convert a single EdgeRule to a List containing one EdgeRule
   */
  implicit def edgeRule2EdgeRuleList(edgeRule: EdgeRule) = {
    List(edgeRule)
  }

  /**
   * Convert a single VertexRule to a List containing one VertexRule
   */
  implicit def vertexRule2VertexRuleList(vertexRule: VertexRule) = {
    List(vertexRule)
  }

  /**
   * Convert a single PropertyRule to a List containing one PropertyRule
   */
  implicit def propertyRule2PropertyRuleList(propertyRule: PropertyRule) = {
    List(propertyRule)
  }

  /**
   * If you want a CompoundValue to start with a StaticValue you should use label().
   *
   * E.g. label("myConstantLabel") + column("columnName")
   */
  def constant(value: Any): ConstantValue = {
    new ConstantValue(value)
  }

  /**
   * Defines a ParsedValue by providing a column name for the src of the Value
   * @param columnName from input row
   */
  def column(columnName: String): ParsedValue = {
    new ParsedValue(columnName)
  }

  /**
   * Define a PropertyRule where the source for the columnName will also be used as the key name in the target
   * @param columnName from input row
   */
  def property(columnName: String): PropertyRule = {
    new PropertyRule(new ConstantValue(columnName), new ParsedValue(columnName))
  }

  /**
   * Create a PropertyRule with a key of "gbId" and a value parsed from the columnName supplied
   * @param columnName from input row
   */
  // TODO: keep? Does anyone ever want their ids called "gbId"
  def gbId(columnName: String): PropertyRule = {
    new PropertyRule(new ConstantValue("gbId"), new ParsedValue(columnName))
  }
}