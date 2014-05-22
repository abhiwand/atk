package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.graphconstruction.{ EdgeRule, VertexRule, OutputConfiguration }

/**
 * NLS TODO:   what are the required pieces of data in the graph interfaces?
 * so far, we just know that it has to have a name
 * @param graphName
 */

case class GraphTemplate(graphName: String,
                         dataFrameId: Long,
                         outputConfig: OutputConfiguration,
                         vertexRules: List[VertexRule],
                         edgeRules: List[EdgeRule],
                         retainDanglingEdges: Boolean,
                         bidirectional: Boolean) {
  require(graphName != null)
  require(graphName.trim.length > 0)

  require(dataFrameId != null)
  require(outputConfig != null)
}

case class Graph(id: Long, name: String) extends HasId {
  require(id > 0)
  require(name != null)
  require(name.trim.length > 0)
}
