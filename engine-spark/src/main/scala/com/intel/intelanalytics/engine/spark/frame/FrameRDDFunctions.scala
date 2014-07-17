package com.intel.intelanalytics.engine.spark.frame

import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._

/**
 * Functions for manipulating frame RDD's and accessing their data.
 */
object FrameRDDFunctions {

  /**
   * Parses data in a column as a double and returns RDD of doubles.
   * @param rowRDD RDD of data rows.
   * @param index Index of the column being parsed.
   * @return RDD of doubles; the result of parsing the specified column as doubles.
   */
  def getColumnAsDoubleRDD(rowRDD: RDD[Row], index: Int): RDD[Double] = {
    try {
      rowRDD.map(row => java.lang.Double.parseDouble(row(index).toString))
    }
    catch {
      case cce: NumberFormatException =>
        throw new NumberFormatException("Column values cannot be parsed as doubles. " + cce.toString)
    }
  }

  /**
   * Parses data in a column as strings and returns RDD of strings.
   * @param rowRDD RDD of data rows.
   * @param index Index of the column being parsed.
   * @return RDD of strings; the result of parsing the specified column as strings.
   */
  def getColumnAsStringRDD(rowRDD: RDD[Row], index: Int): RDD[String] = {
    try {
      rowRDD.map(row => row(index).toString)
    }
    catch {
      case cce: NumberFormatException =>
        throw new NumberFormatException("Column values from cannot be parsed as strings." + cce.toString)
    }
  }
}
