package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.graphconstruction.{ EdgeRule, VertexRule, OutputConfiguration }

/**
 * NLS TODO:   what are the required pieces of data in the graph interfaces?
 * so far, we just know that it has to have a name
 * @param name
 */

case class GraphTemplate(name: String) {
  require(name != null)
  require(name.trim.length > 0)
}

case class Graph(id: Long, name: String) extends HasId {
  require(id > 0)
  require(name != null)
  require(name.trim.length > 0)
}
