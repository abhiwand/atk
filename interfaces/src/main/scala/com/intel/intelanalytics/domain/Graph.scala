package com.intel.intelanalytics.domain

/**
 * Arguments for creating the metadata entry for a graph.
 * @param name The user's name for the graph.
 */

case class GraphTemplate(name: String) {
  require(name != null)
  require(name.trim.length > 0)
}

/**
 * The metadata entry for a graph.
 * @param id The unique identifier of the graph.
 * @param name The user's name for the graph.
 */
case class Graph(id: Long, name: String) extends HasId {
  require(id > 0)
  require(name != null)
  require(name.trim.length > 0)
}
