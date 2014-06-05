package com.intel.intelanalytics.domain.graph

import com.intel.intelanalytics.domain.HasId



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
