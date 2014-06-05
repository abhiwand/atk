package com.intel.intelanalytics.engine

/**
 * Manages multiple graphs in the underlying graph database.
 */
trait GraphStorage {

  def lookup(id: Long): Option[Graph]

  def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal): Graph

  def loadGraph(graph: GraphLoad[JsObject, Long, Long])(implicit user: UserPrincipal): Graph

  def drop(graph: Graph)

  def getGraphs(offset: Int, count: Int)(implicit user: UserPrincipal): Seq[Graph]

}
