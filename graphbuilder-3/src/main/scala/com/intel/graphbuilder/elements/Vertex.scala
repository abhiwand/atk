package com.intel.graphbuilder.elements

/**
 * A Graph Vertex
 *
 * GB Id's are special properties that uniquely identify Vertices.  They are special, in that they must uniquely
 * identify vertices, but otherwise they are completely normal properties.
 *
 * @constructor create a new Vertex
 * @param gbId the unique id that will be used by graph builder
 * @param properties the other properties that exist on this vertex
 */
case class Vertex(gbId: Property, properties: Seq[Property]) extends GraphElement with Mergeable[Vertex] {

  if (gbId == null) {
    throw new IllegalArgumentException("gbId can't be null")
  }

  /**
   * The unique id to used in the groupBy before the merge
   */
  override def id: Any = gbId

  /**
   * Merge two Vertices with the same id into a single Vertex with a combined list of properties.
   * @param other item to merge
   * @return the new merged item
   */
  override def merge(other: Vertex): Vertex = {
    if (id != other.id) {
      throw new IllegalArgumentException("You can merge vertices with different ids")
    }
    new Vertex(gbId, Property.merge(this.properties, other.properties))
  }

  /**
   * Full list of properties including the gbId
   */
  def fullProperties: Seq[Property] = {
    gbId :: properties.toList
  }

}
