package com.intel.graphbuilder.elements

import com.intel.graphbuilder.util.StringUtils

/**
 * A property on a Vertex or Edge.
 *
 * @param key the name of the property
 * @param value the value of the property
 */
case class Property(key: String, value: Any) {

  /**
   * Convenience constructor
   */
  def this(key: Any, value: Any) = {
    this(StringUtils.nullSafeToString(key), value)
  }
}

object Property {

  /**
   * Merge two lists of properties so that keys appear once.
   *
   * Conflicts are handled arbitrarily.
   */
  def merge(listA: Seq[Property], listB: Seq[Property]): Seq[Property] = {
    val listWithDuplicates = listA ++ listB
    val mapWithoutDuplicates = listWithDuplicates.map(p => (p.key, p)).toMap
    mapWithoutDuplicates.valuesIterator.toList
  }

}