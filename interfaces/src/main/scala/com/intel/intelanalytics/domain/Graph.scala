package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.graphconstruction.{ EdgeRule, VertexRule, OutputConfiguration }

/**
 *  TODO:   what are the required pieces of data in the graph interfaces?
 * so far, we just know that it has to have a name... getting the schema from the graph is a tricky thing,
 * since the load step allows the data to determine the schema without telling us what it is ... perhaps
 * the best option would be to set up a "check schema" utility that opens the titan graph, gets its schema
 * and we store it in the metatable, and then dirty it if we modify the graph
 *
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
