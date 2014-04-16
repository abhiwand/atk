package com.intel.graphbuilder.schema

// TODO: we can add support for: isDirected, isManyToOne, sortKey, signature, etc

// TODO: do we want edges with certain labels to only support certain properties, seems like Titan supports this but it wasn't that way in GB2

/**
 * Definition for an Edge Label
 */
case class EdgeLabelDef(label: String) {

  /**
   * Convenience constructor, labels have to be converted to Strings
   */
  def this(label: Any) {
    this(label.toString)
  }

}