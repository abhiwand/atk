package com.intel.intelanalytics.domain.graphconstruction

case class Value(source: String, value: String) {

  require(source.equals("CONSTANT") || source.equals("VARYING"))

  require(value != null)
}
