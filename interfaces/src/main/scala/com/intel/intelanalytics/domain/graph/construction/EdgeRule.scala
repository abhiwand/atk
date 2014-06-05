package com.intel.intelanalytics.domain.graph.construction

/**
 * IntelAnalytics V1 API Graph Loader rule for creating edges from tabular data.
 * @param head Rule for specifying head (destination) vertex of edge.
 * @param tail Rule for specifying tail (source) vertex of edge.
 * @param label Label of the edge.
 * @param properties List of rules for generating properties of the edge.
 * @param bidirectional True if the edge is a bidirectional edge, false if it is a directed edge.
 */
case class EdgeRule(head: PropertyRule,
                    tail: PropertyRule,
                    label: ValueRule,
                    properties: List[PropertyRule],
                    bidirectional: Boolean) {
  require(head != null)
  require(tail != null)
  require(label != null)
}
