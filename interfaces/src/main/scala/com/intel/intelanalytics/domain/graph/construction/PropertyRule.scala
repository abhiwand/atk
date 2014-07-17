package com.intel.intelanalytics.domain.graph.construction

/**
 * IntelAnalytics V1 API Graph Loader rule for creating property graph properties from tabular data.
 * @param key The key of the property to be created.
 * @param value The value of the property to be created.
 */
case class PropertyRule(key: ValueRule, value: ValueRule) {

  require(key != null, "key must not be null")
  require(value != null, "key must not be null")
}
