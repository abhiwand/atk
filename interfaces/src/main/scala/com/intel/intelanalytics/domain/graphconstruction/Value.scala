package com.intel.intelanalytics.domain.graphconstruction

case class Value(source: String, value: String) {
  require(source.equals(GBValueSourcing.CONSTANT) || source.equals(GBValueSourcing.VARYING))
  require(value != null)
}

case object GBValueSourcing {
  val CONSTANT = "CONSTANT"
  val VARYING = "VARYING"
}

