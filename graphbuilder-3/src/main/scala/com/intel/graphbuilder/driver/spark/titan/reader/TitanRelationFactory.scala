package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.elements.{ Edge, Vertex, Property, GraphElement }
import com.tinkerpop.blueprints.Direction
import com.thinkaurelius.titan.core.TitanType
import com.thinkaurelius.titan.graphdb.types.system.SystemType
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx
import scala.collection.mutable.ListBuffer

/**
 * Uses Titan's internal methods to deserialize a single row in a key-value store, and
 * extract a sequence of graph elements containing a vertex and its adjacent edges.
 *
 * @param vertexId Physical vertex ID from the underlying Graph storage layer
 */

class TitanRelationFactory(vertexId: Long) extends com.thinkaurelius.titan.graphdb.database.RelationFactory {
  require(vertexId > 0, "Vertex ID should be greater than zero")

  val gbId = TitanReader.TITAN_READER_GB_ID
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
   * Set the type of the graph element which distinguishes between vertices, edges, and system types
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
   * Set the value of vertex property
   * @param newValue
   */
  override def setValue(newValue: Object) {
    value = newValue
  }

  /**
   * Add edge property
   */
  override def addProperty(newTitanType: TitanType, newValue: Object) {
    properties += (newTitanType.getName() -> newValue)
  }

  /**
   * Deserializes a Titan row, and creates a sequence of GraphBuilder elements containing a vertex
   * and its adjacent edges
   *
   * @see com.thinkaurelius.titan.graphdb.database.EdgeSerializer#readRelation
   *      Titan stores a vertex and its adjacent edges as a single row in the key-value store.
   *      Each vertex property and edge is stored in a distinct column. Titan's deserializer extracts vertex properties
   *      and the adjacent edges from the row.
   *
   *      Once Titan's deserializer extracts properties and edges, the build() method updates
   *      the vertex property list, and edge list.
   *
   * @param titanRow Serialized Titan row
   * @param titanEdgeSerializer Titan's serializer/deserializer
   * @param titanTransaction Titan transaction
   *
   * @return Sequence of graph elements containing vertex and its adjacent edges
   */
  def createGraphElements(titanRow: TitanRow, titanEdgeSerializer: EdgeSerializer, titanTransaction: StandardTitanTx): Seq[GraphElement] = {

    titanRow.serializedEntries.map(entry => {
      titanEdgeSerializer.readRelation(this, entry, titanTransaction);
      build()
    })

    createVertex() match {
      case Some(v) => edgeList :+ v
      case _ => edgeList
    }
  }

  /**
   * Extracts a vertex property or an edge from a column entry, and updates the corresponding
   * vertex property or edge list
   */
  def build(): Unit = {
    if (titanType != null && !isTitanSystemType(titanType)) {
      if (titanType.isPropertyKey()) {
        vertexProperties += new Property(titanType.getName(), value)
      }
      else {
        require(titanType.isEdgeLabel(), "Titan type should be an edge label or a vertex property")
        val edge = createEdge(vertexId, otherVertexID, direction, titanType.getName(), properties)
        if (edge.isDefined) edgeList += edge.get
      }
    }
    properties = Map[String, Any]()
  }

  /**
   * Creates a GraphBuilder vertex from a deserialized Titan vertex
   *
   * @return GraphBuilder vertex
   */
  private def createVertex(): Option[Vertex] = {
    if (vertexProperties.isEmpty) {
      None
    }
    else {
      Option(new Vertex(vertexId, Property(gbId, vertexId), vertexProperties.toSeq))
    }
  }

  /**
   * Creates a GraphBuilder edge from a deserialized Titan edge.
   *
   * Titan represents edges as an adjacency list so each edge gets stored twice. Once in the row containing the
   * outgoing vertex, and again in the row containing the incoming vertex.
   * We limit reads to outgoing edges to prevent duplicates
   *
   * @param vertexId Physical vertex ID from the underlying Graph storage layer
   * @param otherVertexID Physical vertex ID for adjacent vertex in edge
   * @param direction Direction of the edge
   * @param edgeLabel Edge label
   * @param properties Hashmap with edge properties
   *
   * @return GraphBuilder edge
   */
  private def createEdge(vertexId: Long, otherVertexID: Long, direction: Direction, edgeLabel: String, properties: Map[String, Any]): Option[Edge] = {

    direction match {
      case Direction.OUT =>
        val srcVertexId = vertexId
        val destVertexId = otherVertexID
        val edgeProperties = properties.map(entry => Property(entry._1, entry._2)).toSeq

        Option(new Edge(srcVertexId, destVertexId, Property(gbId, srcVertexId), Property(gbId, destVertexId), edgeLabel, edgeProperties))
      case _ => None
    }
  }

  /**
   * Check if the element is a Titan system element. Titan system elements are omitted during deserialization.
   */
  private def isTitanSystemType(newTitanType: TitanType): Boolean = newTitanType.isInstanceOf[SystemType]

}