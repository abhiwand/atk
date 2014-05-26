package com.intel.graphbuilder.driver.spark.titan.reader

import scala.collection.mutable.ListBuffer
import com.intel.graphbuilder.elements.{ Edge, Vertex, Property, GraphElement }
import com.tinkerpop.blueprints.Direction
import com.thinkaurelius.titan.core.TitanType
import com.thinkaurelius.titan.graphdb.types.system.SystemType

/**
 * Used by Titan to deserialize a single row in a key-value store. Titan stores a vertex and its adjacent
 * edges as a single row in the underlying key-value store. Each vertex property and edge is stored in a column.
 * The deserializer extracts vertex properties and the adjacent edges from the columns.
 *
 * @param vertexId Physical vertex ID from the underlying Graph storage layer
 */

class TitanRelationFactory(vertexId: Long) extends com.thinkaurelius.titan.graphdb.database.RelationFactory {

  val gbId = "titanPhysicalId"
  val edgeList = new ListBuffer[GraphElement]
  val vertexProperties = new ListBuffer[Property]

  private var properties = Map[String, Any]()
  private var direction: Direction = null
  private var titanType: TitanType = null
  private var relationID: Long = 0
  private var otherVertexID: Long = 0
  private var value: Object = null

  /**
   * Get Titan's physical ID for vertex
   */
  override def getVertexID(): Long = vertexId

  /**
   * Set direction of edge
   */
  override def setDirection(newDirection: Direction) = {
    direction = newDirection
  }

  /**
   * Set the type of the graph element to distinguish between vertices, edges, and system types
   */
  override def setType(newTitanType: TitanType) = {
    titanType = newTitanType
  }

  /**
   * Set Titan's physical ID for edges
   */
  override def setRelationID(newRelationID: Long) = {
    relationID = newRelationID
  }

  /**
   * Set ID of adjacent vertex in an edge
   */
  override def setOtherVertexID(newOtherVertexID: Long) = {
    otherVertexID = newOtherVertexID
  }

  /**
   * Set the value of vertex properties
   * @param newValue
   */
  override def setValue(newValue: Object) {
    value = newValue
  }

  /**
   * Add vertex and edge properties
   */
  override def addProperty(newTitanType: TitanType, newValue: Object) {
    properties += (newTitanType.getName() -> newValue)
  }

  /**
   * Extracts a vertex property or an edge from a column entry, and updates the corresponding
   * vertex property or edge list
   */
  def build() = {
    if (!isTitanSystemType(titanType)) {
      if (titanType.isPropertyKey()) {
        vertexProperties += new Property(titanType.getName(), value)
      }
      else {
        require(titanType.isEdgeLabel())
        edgeList += createEdge(vertexId, otherVertexID, direction, titanType.getName(), properties)
      }
    }
    properties = Map[String, Any]()
  }

  /**
   * Creates a GraphBuilder vertex from a deserialized Titan vertex
   *
   * @return GraphBuilder vertex
   */
  def createVertex(): Option[Vertex] = {
    if (vertexProperties.isEmpty) {
      None
    }
    else {
      Option(new Vertex(vertexId, Property(gbId, vertexId), vertexProperties.toSeq))
    }
  }

  /**
   * Creates a GraphBuilder edge from a deserialized Titan edge
   *
   * @param vertexId Physical vertex ID from the underlying Graph storage layer
   * @param otherVertexID Physical vertex ID for adjacent vertex in edge
   * @param direction Direction of the edge
   * @param edgeLabel Edge label
   * @param properties Hashmap with edge properties
   *
   * @return GraphBuilder edge
   */
  private def createEdge(vertexId: Long, otherVertexID: Long, direction: Direction, edgeLabel: String, properties: Map[String, Any]): Edge = {
    // TODO: Determine how to handle Direction == BOTH since it is not supported by GraphBuilder.
    // Currently returns edge with Direction.OUT
    val srcVertexId = if (direction == Direction.IN) otherVertexID else vertexId
    val destVertexId = if (direction == Direction.IN) vertexId else otherVertexID

    val edgeProperties = properties.map(entry =>
      Property(entry._1, entry._2)
    ).toSeq

    new Edge(srcVertexId, destVertexId, Property(gbId, srcVertexId), Property(gbId, destVertexId), edgeLabel, edgeProperties)
  }

  /**
   * Check if the element is a Titan system element. Titan system elements are omitted during deserialization.
   */
  private def isTitanSystemType(newTitanType: TitanType): Boolean = newTitanType.isInstanceOf[SystemType]

}