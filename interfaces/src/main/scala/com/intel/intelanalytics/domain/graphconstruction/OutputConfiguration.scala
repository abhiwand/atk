package com.intel.intelanalytics.domain.graphconstruction

case class OutputConfiguration(storeName: String, configuration: Map[String, String]) {
  require(storeName != null)

  // Titan's where it's at for now, baby
  require(storeName == "Titan")
}

