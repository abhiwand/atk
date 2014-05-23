package com.intel.graphbuilder.driver.spark.titan.reader

import scala.collection.mutable.ListBuffer
import com.intel.graphbuilder.elements.{ Edge, Vertex, Property, GraphElement }
import com.tinkerpop.blueprints.Direction
import com.thinkaurelius.titan.core.TitanType
import com.thinkaurelius.titan.graphdb.types.system.{ SystemType, SystemKey }
//import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

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

  var properties  = Map[String, Any]()
  private var direction: Direction = null
  private var titanType: TitanType = null
  private var relationID: Long = 0
  private var otherVertexID: Long = 0
  private var value: Object = null
  private var isSystemType = false

  override def getVertexID(): Long = vertexId

  override def setDirection(newDirection: Direction) = {
    direction = newDirection
  }

  override def setType(newTitanType: com.thinkaurelius.titan.core.TitanType) = {
    if (titanType == SystemKey.TypeClass) {
      isSystemType = true
    }
    titanType = newTitanType
  }

  override def setRelationID(newRelationID: Long) = {
    relationID = newRelationID
  }

  override def setOtherVertexID(newOtherVertexID: Long) = {
    otherVertexID = newOtherVertexID
  }

  override def setValue(newValue: Object) {
    value = newValue
  }

  override def addProperty(newTitanType: TitanType, newValue: Object) {
    properties += (newTitanType.getName() -> newValue)
  }

  /**
   * Extracts a vertex property or edge from a column entry, and updates adjacency list
   */
  def build() = {
    if (!titanType.isInstanceOf[SystemType]) {
      if (titanType.isPropertyKey()) {
        vertexProperties += new Property(titanType.getName(), value)
      }
      else {
        require(titanType.isEdgeLabel())
        edgeList += createEdge(vertexId, otherVertexID, direction, titanType.getName(), properties)
      }
    }
  }

  /**
   * Creates a GraphBuilder vertex from a deserialized Titan vertex
   *
   * @return GraphBuilder vertex
   */
  def createVertex(): Vertex = {
    new Vertex(vertexId, Property(gbId, vertexId), vertexProperties.toSeq)
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
}