package com.intel.intelanalytics.domain.graphconstruction

case class VertexRule(id: Property, properties: List[Property]) {
  require(id != null)
}
