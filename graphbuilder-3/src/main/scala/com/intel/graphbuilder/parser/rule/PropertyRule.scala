package com.intel.graphbuilder.parser.rule

import com.intel.graphbuilder.parser.InputRow

/**
 * A rule definition for how to Parse properties from columns
 *
 * It is helpful to import ValueDSL._ when creating rules
 *
 * @param key the name for the property
 * @param value the value for the property
 */
class PropertyRule(val key: Value, val value: Value) extends Serializable {

  /**
   * Create a simple property where the source columnName is also the destination property name
   * @param columnName from input row
   */
  def this(columnName: String) {
    this(new ConstantValue(columnName), new ParsedValue(columnName))
  }

  /**
   * Does this rule apply to the supplied row
   */
  def appliesTo(row: InputRow): Boolean = {
    value.in(row)
  }
}
