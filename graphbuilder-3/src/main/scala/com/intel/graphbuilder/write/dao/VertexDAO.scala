package com.intel.graphbuilder.write.dao

import com.intel.graphbuilder.elements.{Property, Vertex}
import com.tinkerpop.blueprints
import com.tinkerpop.blueprints.Graph


/**
 * Data access for Vertices using Blueprints API
 */
class VertexDAO(graph: Graph) extends Serializable {

  if (graph == null) {
    throw new IllegalArgumentException("VertexDAO requires a non-null Graph")
  }

  /**
   * Convenience method for looking up a Vertex by either of two ids
   * @param physicalId if not null, perform lookup with this id
   * @param gbId otherwise use this id
   * @return the blueprints Vertex
   */
  def findById(physicalId: AnyRef, gbId: Property): Option[blueprints.Vertex] = {
    if (physicalId != null) findByPhysicalId(physicalId)
    else findByGbId(gbId)
  }

  /**
   * Find a Vertex by the physicalId of the underlying Graph storage layer
   * @param id the physicalId
   */
  def findByPhysicalId(id: AnyRef): Option[blueprints.Vertex] = {
    Option(graph.getVertex(id))
  }

  /**
   * Find a blueprints Vertex by the supplied gbId
   */
  def findByGbId(gbId: Property): Option[blueprints.Vertex] = {
    if (gbId == null) {
      None
    }
    else {
      val vertices = graph.getVertices(gbId.key, gbId.value)
      val i = vertices.iterator()
      if (i.hasNext) {
        Some(i.next())
      } else {
        None
      }
    }
  }

  /**
   * Create a new blueprints.Vertex from the supplied Vertex and set all properties
   * @param vertex the description of the Vertex to create
   * @return the newly created Vertex
   */
  def create(vertex: Vertex): blueprints.Vertex = {
    val blueprintsVertex = graph.addVertex(null)
    update(vertex, blueprintsVertex)
  }

  /**
   * Copy all properties from the supplied GB vertex to the Blueprints Vertex.
   * @param vertex from
   * @param blueprintsVertex to
   * @return the blueprints.Vertex
   */
  def update(vertex: Vertex, blueprintsVertex: blueprints.Vertex): blueprints.Vertex = {
    vertex.fullProperties.map(property => blueprintsVertex.setProperty(property.key, property.value))
    blueprintsVertex
  }

  /**
   * If it exists, find and update the existing vertex, otherwise create a new one
   * @param vertex the description of the Vertex to create
   * @return the newly created Vertex
   */
  def updateOrCreate(vertex: Vertex): blueprints.Vertex = {
    val blueprintsVertex = findByGbId(vertex.gbId)
    if (blueprintsVertex.isEmpty) {
      create(vertex)
    } else {
      update(vertex, blueprintsVertex.get)
    }
  }

}
