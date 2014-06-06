//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.domain

import scala.util.Try

/**
 * Datatypes supported for dataframes, graphs, etc.
 */
object DataTypes {

  /**
   * The datatype trait
   */
  sealed trait DataType {
    type ScalaType

    def parse(s: String): Try[ScalaType]

    def scalaType: Class[ScalaType]
  }

  /**
   * 32 bit ints
   */
  case object int32 extends DataType {
    override type ScalaType = Int

    override def parse(s: String) = Try {
      s.toInt
    }

    override def scalaType = classOf[Int]
  }

  /**
   * 64 bit ints
   */
  case object int64 extends DataType {
    override type ScalaType = Long

    override def parse(s: String): Try[int64.ScalaType] = Try {
      s.toLong
    }

    override def scalaType = classOf[Long]
  }

  /**
   * 32 bit floats
   */
  case object float32 extends DataType {
    override type ScalaType = Float

    override def parse(s: String): Try[float32.ScalaType] = Try {
      s.toFloat
    }

    override def scalaType = classOf[Float]
  }

  /**
   * 64 bit floats
   */
  case object float64 extends DataType {

    override type ScalaType = Double

    override def parse(s: String): Try[float64.ScalaType] = Try {
      s.toDouble
    }

    override def scalaType = classOf[Double]
  }

  /**
   * Strings
   */
  case object string extends DataType {
    override type ScalaType = String

    override def parse(s: String): Try[string.ScalaType] = Try {
      s
    }

    override def scalaType = classOf[String]
  }

  /**
   * An alias for string
   */
  val str = string

  /**
   * An alias for int32
   */
  val int = int32

  /**
   * All the supported datatypes.
   */
  val types: Map[String, DataType] =
    Seq(int32, int64, float32,
      float64, string)
      .map(t => t.toString -> t)
      .toMap ++
      Map("str" -> string,
        "int" -> int32)

  /**
   * Converts a string such as "int32" to a datatype if possible.
   * @param s a string that matches the name of a data type
   * @return the data type object corresponding to the name
   */
  implicit def toDataType(s: String): DataType =
    types.getOrElse(s,
      throw new IllegalArgumentException(s"Invalid datatype: '$s'"))

  /**
   * Converts a data type to a string
   * @param d the data type object
   * @return the name of the object. For example, the int32 data type is named "int32".
   */
  implicit def toString(d: DataType): String = d.toString

  /**
   * Convert an array of strings into an array of values, using the provided schema types
   * to guide conversion.
   *
   * @param columnTypes the types of the columns. The values in the strings array are assumed to be in
   *                    the same order as the values in the columnTypes array.
   * @param strings the strings to be converted
   * @return the convereted values. Any values that cannot be parsed will result in an illegal argument exception.
   */
  def parseMany(columnTypes: Array[DataType])(strings: Array[String]): Array[Any] = {
    val lifted = columnTypes.lift
    strings.zipWithIndex.map {
      case (s, i) => {
        val colType = lifted(i).getOrElse(throw new IllegalArgumentException(
          "Data extend beyond number" +
            " of columns defined in data frame"))
        val value = colType.parse(s)
        value.get
      }
    }
  }
}

import DataTypes._

/**
 * Schema for a data frame. Contains the columns with names and data types.
 * @param columns the columns in the data frame
 */
case class Schema(columns: List[(String, DataType)] = List[(String, DataType)]()) {
  require(columns != null)

  /**
   * get column index by column name
   * @param columnName name of the column to find index
   */
  def columnIndex(columnName: String): Int = columns.indexWhere(columnTuple => columnTuple._1 == columnName)
}

object SchemaUtil {
  /**
   * Resolve naming conflicts when both left and right side of join operation have same column names
   * @param leftColumns columns for the left side of join operation
   * @param rightColumns columns for the right side of join operation
   * @return
   */
  def resolveSchemaNamingConflicts(leftColumns: List[(String, DataType)], rightColumns: List[(String, DataType)]): List[(String, DataType)] = {

    val funcAppendLetterForConflictingNames = (left: List[(String, DataType)], right: List[(String, DataType)], appendLetter: String) => {

      var leftColumnNames = left.map(r => r._1)

      left.map(r =>
        if (right.map(i => i._1).contains(r._1)) {
          var name = r._1 + "_" + appendLetter
          while (leftColumnNames.contains(name)) {
            name = name + "_" + appendLetter
          }
          leftColumnNames = leftColumnNames ++ List(name)
          (name, r._2)
        }
        else
          r
      )
    }

    val left = funcAppendLetterForConflictingNames(leftColumns, rightColumns, "L")
    val right = funcAppendLetterForConflictingNames(rightColumns, leftColumns, "R")

    left ++ right
  }
}
