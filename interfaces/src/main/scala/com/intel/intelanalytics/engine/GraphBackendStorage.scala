package com.intel.intelanalytics.engine

/**
 * This manages the backend storage for graphs, underneath the graph database.
 *
 * The reason that we have to do this is because Titan doesn't provide graph management as part of their interface.
 * So our {@code GraphStorage} component has to know about Titan's backend to clean up the stuff that
 * Titan can not or will not.
 */
trait GraphBackendStorage {
  def deleteUnderlyingTable(graphName: String)
}
