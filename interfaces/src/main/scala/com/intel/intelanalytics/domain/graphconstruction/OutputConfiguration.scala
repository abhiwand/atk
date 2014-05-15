package com.intel.intelanalytics.domain.graphconstruction

case class OutputConfiguration(name: String, configuration: Map[String, String]) {
  require(name != null)
}

