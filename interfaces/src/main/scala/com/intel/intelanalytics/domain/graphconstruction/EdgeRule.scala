package com.intel.intelanalytics.domain.graphconstruction

case class EdgeRule(head: Property, tail: Property, label: Value, properties: Seq[Property]) {
  require(head != null)
  require(tail != null)
  require(label != null)
}
