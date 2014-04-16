package com.intel.graphbuilder.elements

import com.intel.graphbuilder.util.StringUtils

/**
 * A Graph Edge
 *
 * GB Id's are special properties that uniquely identify Vertices.  They are special, in that they must uniquely
 * identify vertices, but otherwise they are completely normal properties.
 *
 * @param tailPhysicalId the unique Physical ID for the source Vertex from the underlying Graph storage layer (optional)
 * @param headPhysicalId the unique Physical ID for the destination Vertex from the underlying Graph storage layer (optional)
 * @param tailVertexGbId the unique ID for the source Vertex
 * @param headVertexGbId the unique ID for the destination Vertex
 * @param label the Edge label
 * @param properties the list of properties associated with this edge
 */
case class Edge(var tailPhysicalId: AnyRef, var headPhysicalId: AnyRef, tailVertexGbId: Property, headVertexGbId: Property, label: String, properties: Seq[Property]) extends GraphElement with Mergeable[Edge] {

  def this(tailVertexGbId: Property, headVertexGbId: Property, label: String, properties: Seq[Property]) {
    this(null, null, tailVertexGbId, headVertexGbId, label, properties)
  }

  def this(tailVertexGbId: Property, headVertexGbId: Property, label: Any, properties: Seq[Property]) {
    this(tailVertexGbId, headVertexGbId, StringUtils.nullSafeToString(label), properties)
  }

  /**
   * The unique id to used in the groupBy before the merge
   */
  override def id: Any = {
    (tailVertexGbId, headVertexGbId, label)
  }

  /**
   * Merge properties for two edges
   * @param other item to merge
   * @return the new merged item
   */
  override def merge(other: Edge): Edge = {
    if (id != other.id) {
      throw new IllegalArgumentException("You can't merge edges with different ids or labels")
    }
    new Edge(tailVertexGbId, headVertexGbId, label, Property.merge(this.properties, other.properties))
  }

  /**
   * Create edge with head and tail in reverse order
   */
  def reverse(): Edge = {
    new Edge(headVertexGbId, tailVertexGbId, label, properties)
  }

}
