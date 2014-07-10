package com.intel.intelanalytics.engine.spark.graph

//TODO: This should be replaced by a "storage" parameter on Graph and DataFrame that holds specifics like
// Titan table name and HDFS uris, etc. for those that need them.

/**
 * Utility for converting between user provided graph names and their names in the graph database.
 */
object GraphName {

  private val iatGraphTablePrefix: String = "iat_graph_"

  /**
   * Converts the user's name for a graph into the name used by the underlying graph store.
   */
  def convertGraphUserNameToBackendName(graphName: String): String = {
    iatGraphTablePrefix + graphName
  }

  /**
   * Converts the name for a graph used by the underlying graph store to the name seen by users.
   */
  def convertGraphBackendNameToUserName(backendName: String): String = {
    backendName.stripPrefix(iatGraphTablePrefix)
  }
}

