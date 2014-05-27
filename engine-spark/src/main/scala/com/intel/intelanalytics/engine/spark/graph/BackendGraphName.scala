package com.intel.intelanalytics.engine.spark.graph

object BackendGraphName {

  val iatGraphTablePrefix: String = "iat_graph_"
  def apply(graphName: String) = { iatGraphTablePrefix + graphName }
}
