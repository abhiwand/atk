package com.intel.graphbuilder.write.dao

import com.intel.graphbuilder.elements.Edge
import com.intel.graphbuilder.elements.Property
import com.tinkerpop.blueprints
import com.tinkerpop.blueprints.{Direction, Graph}

/**
 * Data access for Edges using Blueprints API
 *
 * @param graph the Blueprints Graph
 * @param vertexDAO needed to look up the end points
 */
class EdgeDAO(graph: Graph, vertexDAO: VertexDAO) extends Serializable {

  if (graph == null) {
    throw new IllegalArgumentException("EdgeDAO requires a non-null Graph")
  }

  /**
   * Find the Blueprints Edge using the Vertex ID's and label of the supplied Edge
   * @param edge the GraphBuilder Edge find
   * @return the Blueprints Edge
   */
  def find(edge: Edge): Option[blueprints.Edge] = {
    find(edge.tailVertexGbId, edge.headVertexGbId, edge.label)
  }

  /**
   * Find the Blueprints Edge using the supplied Vertex ID's and label
   * @param tailGbId the source of the Edge
   * @param headGbId the destination of the Edge
   * @param label the Edge label
   * @return the Blueprints Edge
   */
  def find(tailGbId: Property, headGbId: Property, label: String): Option[blueprints.Edge] = {
    val tailVertex = vertexDAO.findByGbId(tailGbId)
    val headVertex = vertexDAO.findByGbId(headGbId)

    if (tailVertex.isEmpty || headVertex.isEmpty) {
      None
    } else {
      find(tailVertex.get, headVertex.get, label)
    }
  }

  /**
   * Find the Blueprints Edge using the supplied parameters.
   * @param tailVertex the source of the Edge
   * @param headVertex the destination of the Edge
   * @param label the Edge label
   * @return the Blueprints Edge
   */
  def find(tailVertex: blueprints.Vertex, headVertex: blueprints.Vertex, label: String): Option[blueprints.Edge] = {
    // iterating over all of the edges seems dumb but I wasn't able to find a better way on Titan forums
    val edgeIterator = tailVertex.query().direction(Direction.OUT).labels(label).edges.iterator()
    while (edgeIterator.hasNext) {
      val blueprintsEdge = edgeIterator.next()
      if (blueprintsEdge.getVertex(Direction.IN) == headVertex) {
        return Some(blueprintsEdge)
      }
    }
    None
  }

  /**
   * Create a new blueprints.Edge from the supplied Edge and set all properties
   * @param edge the description of the Edge to create
   * @return the newly created Edge
   */
  def create(edge: Edge): blueprints.Edge = {
    val tailVertex = vertexDAO.findById(edge.tailPhysicalId, edge.tailVertexGbId)
    val headVertex = vertexDAO.findById(edge.headPhysicalId, edge.headVertexGbId)
    if (tailVertex.isEmpty || headVertex.isEmpty) {
      throw new IllegalArgumentException("Vertex was missing, can't insert edge: " + edge)
    }
    val blueprintsEdge = graph.addEdge(null, tailVertex.get, headVertex.get, edge.label)
    update(edge, blueprintsEdge)
  }

  /**
   * Copy all properties from the supplied GB Edge to the Blueprints Edge
   * @param edge from
   * @param blueprintsEdge to
   * @return the blueprints.Edge
   */
  def update(edge: Edge, blueprintsEdge: blueprints.Edge): blueprints.Edge = {
    edge.properties.map(property => blueprintsEdge.setProperty(property.key, property.value))
    blueprintsEdge
  }

  /**
   * If it exists, find and update the existing edge, otherwise create a new one
   * @param edge the description of the Edge to create
   * @return the newly created Edge
   */
  def updateOrCreate(edge: Edge): blueprints.Edge = {
    val blueprintsEdge = find(edge)
    if (blueprintsEdge.isEmpty) {
      create(edge)
    }
    else {
      update(edge, blueprintsEdge.get)
    }
  }

}
