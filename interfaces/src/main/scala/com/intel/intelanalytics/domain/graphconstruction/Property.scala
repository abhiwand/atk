package com.intel.intelanalytics.domain.graphconstruction


case class Property(key: Value, value: Value) {

  require(key != null)
  require(value != null)
}
