package com.intel.intelanalytics.engine.spark.graph

/**
 * Stores constants related to graph storage.
 */
case object iatGraphStorageConstants {
  val iatGraphTablePrefix: String = "iat_graph_"
}

/**
 * Converts the user's name for a graph into the name used by the underlying graph store.
 */
object ConvertGraphUserNameToBackendName {

  def apply(graphName: String) = {
    iatGraphStorageConstants.iatGraphTablePrefix + graphName
  }
}

/**
 * Converts the name for a graph used by the underlying graph store to the name seen by users.
 */
object ConvertGraphBackendNameToUserName {

  def apply(backendName: String) = backendName.stripPrefix(iatGraphStorageConstants.iatGraphTablePrefix)
}
