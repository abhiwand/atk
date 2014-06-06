package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.graph.{GraphLoad, GraphTemplate, Graph}
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.JsObject

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
