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

package com.intel.graphbuilder.parser.rule

/**
 * Methods and implicit conversions that help the creation of ParserRules to be simpler and use a more DSL-like syntax
 */
object RuleParserDSL {

  /**
   * Convert Any to a ConstantValue
   */
  implicit def anyToConstantValue(value: Any) = {
    new ConstantValue(value)
  }

  /**
   * Convert a single EdgeRule to a List containing one EdgeRule
   */
  implicit def edgeRuleToEdgeRuleList(edgeRule: EdgeRule) = {
    List(edgeRule)
  }

  /**
   * Convert a single VertexRule to a List containing one VertexRule
   */
  implicit def vertexRuleToVertexRuleList(vertexRule: VertexRule) = {
    List(vertexRule)
  }

  /**
   * Convert a single PropertyRule to a List containing one PropertyRule
   */
  implicit def propertyRuleToPropertyRuleList(propertyRule: PropertyRule) = {
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
