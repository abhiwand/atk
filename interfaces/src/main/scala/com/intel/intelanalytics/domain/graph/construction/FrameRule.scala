package com.intel.intelanalytics.domain.graph.construction

import com.intel.intelanalytics.domain.frame.FrameReference

/**
 * Determines how to convert a dataframe into graph data.
 * @param frame Handle to the dataframe being read from.
 * @param vertex_rules Rules for interpreting tabular data as vertices.
 * @param edge_rules Rules fo interpreting tabluar data as edges.
 */
case class FrameRule(frame: FrameReference, vertex_rules: List[VertexRule], edge_rules: List[EdgeRule]) {
}
