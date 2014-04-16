package com.intel.graphbuilder.elements

/**
 * Mergeable items can be combined into a new single item.
 * <p>
 * In the case of Graph Elements, merging means creating a new Graph Element that has a combined set of properties.
 * </p>
 */
trait Mergeable[T] {

  /**
   * Merge-ables with the same id can be merged together.
   *
   * (In Spark, you would use this as the unique id in the groupBy before merging duplicates)
   */
  def id: Any

  /**
   * Merge two items into one.
   *
   * @param other item to merge
   * @return the new merged item
   */
  def merge(other: T): T

}
