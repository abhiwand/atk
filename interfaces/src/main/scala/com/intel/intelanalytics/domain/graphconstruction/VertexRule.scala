package com.intel.intelanalytics.domain.graphconstruction

case class VertexRule(id: Property, properties: Seq[Property]) {
  require(id != null)
}
