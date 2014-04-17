package com.intel.graphbuilder.schema

import org.apache.commons.lang3.StringUtils

/**
 * A property definition is either of type Edge or Vertex
 */
object PropertyType extends Enumeration {
  val Vertex, Edge = Value
}

/**
 * Schema definition for a Property
 *
 * @param propertyType this property is either for a Vertex or an Edge
 * @param name property name
 * @param dataType data type
 * @param unique True if this property is unique
 * @param indexed True if this property should be indexed
 */
case class PropertyDef(propertyType: PropertyType.Value, name: String, dataType: Class[_], unique: Boolean, indexed: Boolean) {

  if (StringUtils.isEmpty(name)) {
    throw new IllegalArgumentException("property name can't be empty")
  }

  // TODO: in the future, possibly add support for indexName

}