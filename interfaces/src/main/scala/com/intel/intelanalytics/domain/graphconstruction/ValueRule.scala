package com.intel.intelanalytics.domain.graphconstruction

/**
 * Values for graph loading can either be constants or come from tabular. In the latter case, we call them
 * VARYING.
 */
case object GBValueSourcing {
  val CONSTANT = "CONSTANT"
  val VARYING = "VARYING"
}

/**
 * IntelAnalytics V1 API Graph Loader rule for creating property graph values from tabular data.
 * @param source Is the value constant or varying?
 * @param value If a constant, the value taken. If a varying, the name of the column from which the data is parsed.
 */
case class ValueRule(source: String, value: String) {
  require(source.equals(GBValueSourcing.CONSTANT) || source.equals(GBValueSourcing.VARYING))
  require(value != null)
}

