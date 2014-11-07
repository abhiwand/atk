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
import spray.json.JsValue

import spray.json._
import DefaultJsonProtocol._
/**
 * Datatypes supported for frames, graphs, etc.
 */
object DataTypes {

  /**
   * The datatype trait
   */
  sealed trait DataType {
    type ScalaType

    def parse(raw: Any): Try[ScalaType]

    /** True if the supplied value matches this data type */
    def isType(raw: Any): Boolean

    def scalaType: Class[ScalaType]

    def typedJson(raw: Any): JsValue

    def asDouble(raw: Any): Double

    def isNumerical: Boolean
  }

  /**
   * 32 bit ints
   */
  case object int32 extends DataType {
    override type ScalaType = Int

    override def parse(raw: Any) = Try {
      toInt(raw)
    }

    override def isType(raw: Any): Boolean = {
      // TODO: nulls aren't allowed for now but we will need to support nulls or Nones later
      raw != null && raw.isInstanceOf[Int]
    }

    override def scalaType = classOf[Int]

    override def typedJson(raw: Any) = {
      raw.asInstanceOf[Int].toJson
    }

    override def asDouble(raw: Any): Double = {
      raw.asInstanceOf[Int].toDouble
    }

    override def isNumerical = true
  }

  /**
   * 64 bit ints
   */
  case object int64 extends DataType {
    override type ScalaType = Long

    override def parse(raw: Any) = Try {
      toLong(raw)
    }

    override def isType(raw: Any): Boolean = {
      // TODO: nulls aren't allowed for now but we will need to support nulls or Nones later
      raw != null && raw.isInstanceOf[Long]
    }

    override def scalaType = classOf[Long]

    override def typedJson(raw: Any) = {
      raw.asInstanceOf[Long].toJson
    }

    override def asDouble(raw: Any): Double = {
      raw.asInstanceOf[Long].toDouble
    }

    override def isNumerical = true
  }

  /**
   * 32 bit floats
   */
  case object float32 extends DataType {
    override type ScalaType = Float

    override def parse(raw: Any) = Try {
      toFloat(raw)
    }

    override def isType(raw: Any): Boolean = {
      // TODO: nulls aren't allowed for now but we will need to support nulls or Nones later
      raw != null && raw.isInstanceOf[Float]
    }

    override def scalaType = classOf[Float]

    override def typedJson(raw: Any) = {
      raw.asInstanceOf[Float].toJson
    }

    override def asDouble(raw: Any): Double = {
      raw.asInstanceOf[Float].toDouble
    }

    override def isNumerical = true
  }

  /**
   * 64 bit floats
   */
  case object float64 extends DataType {

    override type ScalaType = Double

    override def parse(raw: Any) = Try {
      toDouble(raw)
    }

    override def isType(raw: Any): Boolean = {
      // TODO: nulls aren't allowed for now but we will need to support nulls or Nones later
      raw != null && raw.isInstanceOf[Double]
    }

    override def scalaType = classOf[Double]

    override def typedJson(raw: Any) = {
      raw.asInstanceOf[Double].toJson
    }

    override def asDouble(raw: Any): Double = {
      raw.asInstanceOf[Double]
    }

    override def isNumerical = true
  }

  /**
   * Strings
   */
  case object string extends DataType {
    override type ScalaType = String

    override def parse(raw: Any) = Try {
      toStr(raw)
    }

    override def isType(raw: Any): Boolean = {
      // where null is allowed we accept null as this type
      raw == null || raw.isInstanceOf[String]
    }

    override def scalaType = classOf[String]

    override def typedJson(raw: Any) = {
      raw.asInstanceOf[String].toJson
    }

    override def asDouble(raw: Any): Double = {
      try {
        java.lang.Double.parseDouble(raw.asInstanceOf[String])
      }
      catch {
        case e: Exception => throw new IllegalArgumentException("Could not parse " + raw + " as a Double.")
      }
    }

    override def isNumerical = false
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
   * All the supported datatypes
   */
  val supportedTypes: Map[String, DataType] =
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
    supportedTypes.getOrElse(s,
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
   * @param values the strings to be converted
   * @return the converted values. Any values that cannot be parsed will result in an illegal argument exception.
   */
  def parseMany(columnTypes: Array[DataType])(values: Array[Any]): Array[Any] = {
    val frameColumnCount = columnTypes.length
    val dataCount = values.length

    if (frameColumnCount != dataCount)
      throw new IllegalArgumentException(s"Expected $frameColumnCount columns, but got $dataCount columns in this row.")

    val lifted = columnTypes.lift
    values.zipWithIndex.map {
      case (s, i) => {
        s match {
          case null => null
          case _ =>
            val colType = lifted(i).get
            val value = colType.parse(s)
            value.get
        }
      }
    }
  }

  /**
   * Get a String description of the data type of the supplied value
   */
  def dataTypeOfValueAsString(value: Any): String = {
    if (value == null) {
      "null"
    }
    else {
      // TODO: we should convert this to our type names
      value.getClass.getName
    }
  }

  def dataTypeOfValue(value: Any): DataType = {
    val matches = supportedTypes.filter { case (name: String, dataType: DataType) => dataType.isType(value) }.toList
    if (matches.isEmpty) {
      throw new IllegalArgumentException("No matching data type found for value: " + value)
    }
    else if (matches.length > 1) {
      error("")
    }
    matches.head._2
  }

  /**
   * Convert any type to any other type.
   *
   * Throw errors for conditions not supported (for example, string "s" cannot convert to a Long)
   */
  def convertToType(value: Any, dataType: DataType): Any = {
    dataType match {
      case `int64` => toLong(value)
      case `float64` => toDouble(value)
      // TODO: finish implementation (sorry, I only implemented the minimal I needed)
      // TODO: throw exceptions when needed
      case _ => throw new RuntimeException("DataTypes.convertToType() Not yet implemented")
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
      case null => throw new IllegalArgumentException("null cannot be converted to Double")
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case f: Float => f.toDouble
      case d: Double => d
      case bd: BigDecimal => bd.toDouble
      case s: String => s.trim().toDouble
      case _ => throw new IllegalArgumentException(s"The following value is not a numeric data type: $value")
    }
  }

  def toBigDecimal(value: Any): BigDecimal = {
    value match {
      case null => null
      case i: Int => BigDecimal(i)
      case l: Long => BigDecimal(l)
      case f: Float => BigDecimal(f)
      case d: Double => BigDecimal(d)
      case bd: BigDecimal => bd
      case s: String => BigDecimal(s)
      case _ => throw new IllegalArgumentException(s"The following value is not of numeric data type: $value")
    }
  }

  def toLong(value: Any): Long = {
    value match {
      case null => throw new IllegalArgumentException("null cannot be converted to Long")
      case i: Int => i.toLong
      case l: Long => l
      case f: Float => f.toLong
      case d: Double => d.toLong
      case bd: BigDecimal => bd.toLong
      case s: String => s.trim().toLong
      case _ => throw new RuntimeException("Not yet implemented")
    }
  }

  def toInt(value: Any): Int = {
    value match {
      case null => throw new IllegalArgumentException("null cannot be converted to Int")
      case i: Int => i
      case l: Long => l.toInt
      case f: Float => f.toInt
      case d: Double => d.toInt
      case bd: BigDecimal => bd.toInt
      case s: String => s.trim().toInt
      case _ => throw new RuntimeException("Not yet implemented")
    }
  }

  def toFloat(value: Any): Float = {
    value match {
      case null => throw new IllegalArgumentException("null cannot be converted to Float")
      case i: Int => i.toFloat
      case l: Long => l.toFloat
      case f: Float => f
      case d: Double => d.toFloat
      case bd: BigDecimal => bd.toFloat
      case s: String => s.trim().toFloat
      case _ => throw new RuntimeException("Not yet implemented")
    }
  }

  def toStr(value: Any): String = {
    value match {
      case null => null
      case i: Int => i.toString
      case l: Long => l.toString
      case f: Float => f.toString
      case d: Double => d.toString
      case bd: BigDecimal => bd.toString()
      case s: String => s
      case _ => throw new RuntimeException("Not yet implemented")
    }
  }

  /**
   * Compare our supported data types
   * @param valueA
   * @param valueB
   * @return
   */
  def compare(valueA: Any, valueB: Any): Int = {
    if (valueB == null) {
      if (valueA == null) {
        0
      }
      else {
        1
      }
    }
    else {
      valueA match {
        case null => -1
        case i: Int => i.compare(DataTypes.toInt(valueB))
        case l: Long => l.compare(DataTypes.toLong(valueB))
        case f: Float => f.compare(DataTypes.toFloat(valueB))
        case d: Double => d.compare(DataTypes.toDouble(valueB))
        case s: String => s.compareTo(valueB.toString)
        case _ => throw new RuntimeException("Not yet implemented")
      }
    }

  }

}
