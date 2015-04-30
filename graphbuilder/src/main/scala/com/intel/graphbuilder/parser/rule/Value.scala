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

import com.intel.graphbuilder.parser.InputRow
import org.apache.commons.lang3.StringUtils

/**
 * With abstract values we can construct rules that can describe Edges and Vertices with
 * hard-coded static values, parsed values, and other types of values.
 *
 * For example, an Edge label could have either a constant value like 'age' or a dynamic
 * value parsed from the input like "23".
 */
abstract class Value {

  /**
   * True if this value gets parsed from the Input
   */
  def isParsed: Boolean

  /**
   * True if this value is NOT parsed from the Input
   */
  final def isNotParsed = !isParsed

  /**
   * True if this value is available (meaning it is either a constant or available from the input row)
   */
  def in(row: InputRow): Boolean

  /**
   * The actual value represented by this abstract value.  This method should only be called is isParsed is false.
   * Otherwise a row needs to be supplied.
   */
  def value: Any

  /**
   * Always gives the value, either parsed from the supplied InputRow and/or the immediate data, if isParsed is false
   */
  def value(row: InputRow): Any

  /**
   * Concatenate this value with another, creating a new compound value.
   */
  def +(value: Value): Value = {
    new CompoundValue(this, value)
  }

  /**
   * PropertyRules can be defined from values using a -> syntax similar to Maps.
   *
   * For example, constant("keyNameInOutput") -> column("columnNameFromInput") will define a PropertyRule the
   * same as new PropertyRule(new ConstantValue("keyNameInOutput"), new ParsedValue("columnNameFromInput"))
   */
  def ->(value: Value): PropertyRule = {
    new PropertyRule(this, value)
  }
}

/**
 * A statically defined value, like a String that never changes.
 *
 * Constant values do not come from the InputRow.
 */
case class ConstantValue(value: Any) extends Value {

  /**
   * ConstantValues are never considered parsed
   */
  def isParsed: Boolean = false

  /**
   * ConstantValue are always considered "in" the row.
   */
  def in(row: InputRow): Boolean = true

  /**
   * Give the value.
   *
   * Note: ConstantValues always ignore the supplied parameter but this method is nice for the API consistency
   * it gives elsewhere.
   */
  def value(row: InputRow): Any = {
    value
  }

}

/**
 * A value that is dynamically parsed from a column in the InputRow.
 */
case class ParsedValue(columnName: String) extends Value {

  if (StringUtils.isEmpty(columnName)) {
    throw new IllegalArgumentException("Column name cannot be null or empty")
  }

  /**
   * ParsedValues are always considered parsed.
   */
  def isParsed: Boolean = true

  /**
   * True if the column specified is a non-empty column.
   */
  def in(row: InputRow): Boolean = {
    row.columnIsNotEmpty(columnName)
  }

  /**
   * Always throws an Exception for ParsedValue
   */
  def value: Any = {
    throw new RuntimeException("value() method is not valid on ParsedValues, please check isParsed() before "
      + " calling value.  ParsedValues need to be passed an InputRow.")
  }

  /**
   * Get the value from the Row
   */
  def value(row: InputRow): Any = {
    row.value(columnName)
  }

}

/**
 * A CompoundValue is a composite made up of two values.
 *
 * String concatenation is the assumed output.
 *
 * @param val1 the left hand value
 * @param val2 the right hand value
 */
case class CompoundValue(val1: Value, val2: Value) extends Value {

  /**
   * If all values are constants, isParsed returns false.
   * If any value is parsed, isParsed returns true.
   */
  def isParsed: Boolean = {
    val1.isParsed || val2.isParsed
  }

  /**
   * True if both values are considered "in" the InputRow.
   */
  def in(row: InputRow): Boolean = {
    val1.in(row) && val2.in(row)
  }

  /**
   * If isParsed is true, this method will always throw an Exception.
   * Otherwise, it provides the concatenated value.
   */
  def value: Any = {
    toString(val1.value) + val2.value
  }

  /**
   * Always gives the concatenated value, either parsed from the supplied InputRow, and/or otherwise,
   * depending on the types of Values.
   *
   * String concatenation is the assumed output.
   */
  def value(row: InputRow): Any = {
    toString(val1.value(row)) + val2.value(row)
  }

  /** Convert Any to a String, handling null by providing an EMPTY_STRING */
  private def toString(any: Any) = {
    if (any == null) StringUtils.EMPTY
    else any.toString
  }
}
