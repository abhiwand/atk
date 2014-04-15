package com.intel.graphbuilder.schema

/**
 * Represents the definition of a Graph Schema
 *
 * @param edgeLabelDefs edge label definitions (label names)
 * @param propertyDefs property definitions (names, indexes, uniqueness)
 */
class GraphSchema(val edgeLabelDefs: List[EdgeLabelDef], val propertyDefs: List[PropertyDef]) {

  /**
   * Get a list of zero or more properties with the supplied name
   */
  def propertiesWithName(propertyName: String): List[PropertyDef] = {
    propertyDefs.filter(propertyDef => propertyDef.name == propertyName)
  }

}