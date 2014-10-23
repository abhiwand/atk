package com.intel.intelanalytics.engine.spark.frame.plugins.load

/**
 * Parsing results into two types of rows: successes and failures.
 *
 * Successes go into one data frame and errors go into another.
 *
 * @param parseSuccess true if this row was a success, false otherwise
 * @param row either the successfully parsed row OR a row of two columns (original line and error message)
 */
case class RowParseResult(parseSuccess: Boolean, row: Array[Any]) {
  if (!parseSuccess) {
    require(row.length == 2, "error rows have two columns: the original un-parsed line and the error message")
  }
}
