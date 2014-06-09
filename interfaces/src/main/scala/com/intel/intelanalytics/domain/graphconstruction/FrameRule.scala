package com.intel.intelanalytics.domain.graphconstruction

/**
 * Determines how to convert a dataframe into graph data.
 * @param frame Handle to the dataframe being read from.
 * @param vertex_rules Rules for interpreting tabular data as vertices.
 * @param edge_rules Rules fo interpreting tabluar data as edges.
 * @tparam FrameRef The type of the handle to the dataframe.
 */
case class FrameRule[FrameRef](frame: FrameRef, vertex_rules: List[VertexRule], edge_rules: List[EdgeRule]) {
}
