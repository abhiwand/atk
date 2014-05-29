package com.intel.intelanalytics.domain.graphconstruction

/**
 * IntelAnalytics V1 API Graph Loader rule for creating edges from tabular data.
 * @param head Rule for specifying head (destination) vertex of edge.
 * @param tail Rule for specifying tail (source) vertex of edge.
 * @param label Label of the edge.
 * @param properties List of rules for generating properties of the edge.
 */
case class EdgeRule(head: PropertyRule, tail: PropertyRule, label: ValueRule, properties: List[PropertyRule]) {
  require(head != null)
  require(tail != null)
  require(label != null)
}
