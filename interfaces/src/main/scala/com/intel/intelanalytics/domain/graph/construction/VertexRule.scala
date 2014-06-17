package com.intel.intelanalytics.domain.graph.construction

/**
 * IntelAnalytics V1 API Graph Loader rule for creating vertices from tabular data.
 * @param id Rule for specifying the ID of the vertex.
 * @param properties List of rules for specifying properties of the vertices.
 */
case class VertexRule(id: PropertyRule, properties: List[PropertyRule]) {
  require(id != null)
}
