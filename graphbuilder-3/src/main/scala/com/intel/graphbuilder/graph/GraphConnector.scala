package com.intel.graphbuilder.graph

import com.tinkerpop.blueprints.TransactionalGraph

/**
 * Interface for connecting to a Graph.
 */
trait GraphConnector {

  /**
   * Get a connection to a graph database
   */
  def connect(): TransactionalGraph

}