package com.intel.intelanalytics.domain.graphconstruction

/**
 * IntelAnalytics V1 API Graph Loader rule for specifying database configuration during write.
 * @param storeName Which graph database service to use. (Only "Titan" for now.)
 * @param configuration Map of configuration settings to be consumed by the graph database.
 */
case class OutputConfiguration(storeName: String, configuration: Map[String, String]) {
  require(storeName != null)

  require(storeName == "Titan") // if other stores become supported we can relax this requirement
}

