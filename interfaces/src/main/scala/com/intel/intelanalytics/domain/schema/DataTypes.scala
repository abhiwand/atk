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

package com.intel.intelanalytics.domain.schema

import scala.collection.immutable.Set
import scala.util.Try

/**
 * Datatypes supported for dataframes, graphs, etc.
 */
object DataTypes {

  val pythonRddNullString = "YoMeNull" // TODO - identify correct null indicator, and locate this appropriately

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
   * An alias for string
   */
  val unicode = string

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
        "unicode" -> string,
        "int" -> int32)

  /**
   * Determine root DataType that all DataTypes in list can be converted into.
   * @param dataTypes DataTypes to merge
   * @return  Merged DataType
   */
  def mergeTypes(dataTypes: List[DataType]): DataType = {
    dataTypes.toSet match {
      case x if x.size == 1 => x.head
      case x if Set[DataType](string).subsetOf(x) => string
      case x if Set[DataType](float64).subsetOf(x) => float64
      case x if Set[DataType](int64, float32).subsetOf(x) => float64
      case x if Set[DataType](int32, float32).subsetOf(x) => float32
      case x if Set[DataType](int32, int64).subsetOf(x) => int64
      case _ => string
    }
  }

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
   * @return the converted values. Any values that cannot be parsed will result in an illegal argument exception.
   */
  def parseMany(columnTypes: Array[DataType])(strings: Array[String]): Array[Any] = {
    val lifted = columnTypes.lift
    strings.zipWithIndex.map {
      case (s, i) => {
        s match {
          case nullString if nullString == pythonRddNullString => null // TODO - remove, handle differently
          case _ =>
            val colType = lifted(i).getOrElse(throw new IllegalArgumentException(
              "Data extend beyond number" +
                " of columns defined in data frame"))
            val value = colType.parse(s)
            value.get
        }
      }
    }
  }

  /**
   * Attempt to cast Any type to Double
   *
   * @param value input Any type to be cast
   * @return value cast as Double, if possible
   */
  def toDouble(value: Any): Double = {
    value match {
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case f: Float => f.toDouble
      case d: Double => d
      case _ => throw new IllegalArgumentException(s"The following value is not a numeric data type: $value")
    }
  }

}
