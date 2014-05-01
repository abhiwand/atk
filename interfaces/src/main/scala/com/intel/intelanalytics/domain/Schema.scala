package com.intel.intelanalytics.domain

import scala.util.Try

object DataTypes {

  sealed trait DataType {
    type ScalaType

    def parse(s: String): Try[ScalaType]
  }

  case object int32 extends DataType {
    override type ScalaType = Int

    override def parse(s: String) = Try {
      s.toInt
    }
  }

  case object int64 extends DataType {
    override type ScalaType = Long

    override def parse(s: String): Try[int64.ScalaType] = Try {
      s.toLong
    }
  }

  case object float32 extends DataType {
    override type ScalaType = Float

    override def parse(s: String): Try[float32.ScalaType] = Try {
      s.toFloat
    }
  }

  case object float64 extends DataType {

    override type ScalaType = Double

    override def parse(s: String): Try[float64.ScalaType] = Try {
      s.toDouble
    }
  }

  case object string extends DataType {
    override type ScalaType = String

    override def parse(s: String): Try[string.ScalaType] = Try {
      s
    }
  }

  val str = string

  val int = int32

  val types: Map[String, DataType] =
    Seq(int32, int64, float32,
      float64, string)
      .map(t => t.toString -> t)
      .toMap ++
      Map("str" -> string,
        "int" -> int32)

  implicit def toDataType(s: String): DataType =
    types.getOrElse(s,
      throw new IllegalArgumentException(s"Invalid datatype: '$s'"))

  implicit def toString(d: DataType): String = d.toString

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

case class Schema(columns: List[(String, DataType)]) {
  require(columns != null)
}
