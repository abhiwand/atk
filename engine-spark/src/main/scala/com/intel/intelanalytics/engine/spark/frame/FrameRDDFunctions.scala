package com.intel.intelanalytics.engine.spark.frame

import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._

/**
 * Functions for manipulating frame RDD's and accessing their data.
 */
object FrameRDDFunctions {

  /**
   * Parses data in a column as doubles and returns RDD of the doubles.
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
   * Parses data in a column as float32s and returns RDD of the float32s.
   * @param rowRDD RDD of data rows.
   * @param index Index of the column being parsed.
   * @return RDD of float32s; the result of parsing the specified column as float32s.
   */
  def getColumnAsFloatRDD(rowRDD: RDD[Row], index: Int): RDD[Float] = {
    try {
      rowRDD.map(row => java.lang.Float.parseFloat(row(index).toString))
    }
    catch {
      case cce: NumberFormatException =>
        throw new NumberFormatException("Column values cannot be parsed as floats. " + cce.toString)
    }
  }

  /**
   * Parses data in a column as longs and returns RDD of the longs.
   * @param rowRDD RDD of data rows.
   * @param index Index of the column being parsed.
   * @return RDD of longs; the result of parsing the specified column as longs.
   */
  def getColumnAsLongRDD(rowRDD: RDD[Row], index: Int): RDD[Long] = {
    try {
      rowRDD.map(row => java.lang.Long.parseLong(row(index).toString))
    }
    catch {
      case cce: NumberFormatException =>
        throw new NumberFormatException("Column values cannot be parsed as Longs. " + cce.toString)
    }
  }

  /**
   * Parses data in a column as ints and returns RDD of the ints.
   * @param rowRDD RDD of data rows.
   * @param index Index of the column being parsed.
   * @return RDD of longs; the result of parsing the specified column as ints.
   */
  def getColumnAsIntRDD(rowRDD: RDD[Row], index: Int): RDD[Int] = {
    try {
      rowRDD.map(row => java.lang.Integer.parseInt(row(index).toString))
    }
    catch {
      case cce: NumberFormatException =>
        throw new NumberFormatException("Column values cannot be parsed as integers. " + cce.toString)
    }
  }

  /**
   * Parses data in a column as strings and returns RDD of the strings.
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
