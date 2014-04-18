package com.intel.intelanalytics.domain

object SchemaValidation {
  val types = Array("int", "string", "int32", "int64", "float32", "float64", "str")
}

case class Schema(columns: List[(String,String)]) {
  require(columns != null)
  for(c <- columns) {
    val dataType: String = c._2
    require(SchemaValidation.types.contains(dataType), s"Invalid datatype: $dataType")
  }
}
