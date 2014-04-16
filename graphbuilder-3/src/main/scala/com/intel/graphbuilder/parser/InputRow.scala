package com.intel.graphbuilder.parser

import org.apache.commons.lang3.StringUtils

/**
 * Wrapper for rows to simplify parsing.
 */
class InputRow(inputSchema: InputSchema, row: Seq[Any]) {

  if (row.size != inputSchema.size) {
    throw new IllegalArgumentException("Input row should have the same number of columns as the inputSchema")
  }

  def value(columnName: String): Any = {
    row(inputSchema.columnIndex(columnName))
  }

  /**
   * Column is considered empty if it is null, None, Nil, or the empty String
   */
  def columnIsNotEmpty(columnName: String): Boolean = {
    val any = row(inputSchema.columnIndex(columnName))
    any match {
      case null => false
      case None => false
      case Nil => false
      case s: String => StringUtils.isNotEmpty(s)
      case _ => true
    }
  }

}