package com.intel.intelanalytics.domain.graph

import com.intel.intelanalytics.domain.Naming

object GraphName {

  def validate(name: String): String = Naming.validateAlphaNumericUnderscore(name)

  def validateOrGenerate(name: Option[String]): String = Naming.validateAlphaNumericUnderscoreOrGenerate(name, { generate() })

  /**
   * Automatically generate a unique name for a graph.
   *
   * The frame name comprises of the prefix "iat_graph", a random uuid, and an optional annotation.
   *
   * @param annotation Optional annotation to add to graph name
   * @return Graph name
   */
  def generate(annotation: Option[String] = None): String = Naming.generateName(Some("graph_"), annotation)
}