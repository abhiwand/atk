package com.intel.intelanalytics.domain.graph.construction

import com.intel.intelanalytics.domain.frame.FrameReference

/**
 * Determines how to convert a dataframe into graph data.
 * @param frame Handle to the dataframe being read from.
 * @param vertexRules Rules for interpreting tabular data as vertices.
 * @param edgeRules Rules fo interpreting tabluar data as edges.
 */
case class FrameRule(frame: FrameReference, vertexRules: List[VertexRule], edgeRules: List[EdgeRule]) {
}
