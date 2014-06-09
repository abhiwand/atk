package com.intel.intelanalytics.domain.graph.construction



/**
 * IntelAnalytics V1 API Graph Loader rule for creating property graph values from tabular data.
 * @param source Is the value constant or varying?
 * @param value If a constant, the value taken. If a varying, the name of the column from which the data is parsed.
 */
case class ValueRule(source: String, value: String) {
  require(source.equals(GBValueSourcing.CONSTANT) || source.equals(GBValueSourcing.VARYING))
  require(value != null)
}

