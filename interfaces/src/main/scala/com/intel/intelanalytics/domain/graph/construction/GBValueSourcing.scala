package com.intel.intelanalytics.domain.graph.construction

/**
 * Values for graph loading can either be constants or come from tabular. In the latter case, we call them
 * VARYING.
 */
case object GBValueSourcing {
  val CONSTANT = "CONSTANT"
  val VARYING = "VARYING"
}
