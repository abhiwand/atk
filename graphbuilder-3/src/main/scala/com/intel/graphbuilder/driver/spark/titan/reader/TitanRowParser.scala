
package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.elements.{GraphElement, Property}
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler
import scala.collection.mutable.ListBuffer

/**
 * Parses a row in Titan's key-value store. Each row represents a vertex and its adjacent edges
 *
 * @param titanRow Serialized Titan row
 * @param titanEdgeSerializer Titan's serializer/deserializer
 * @param titanTransaction Titan transaction
 */
case class TitanRowParser(titanRow: TitanRow, titanEdgeSerializer: EdgeSerializer, titanTransaction: StandardTitanTx) {

  val vertexId = IDHandler.getKeyID(titanRow.rowKey)
  val vertexProperties = new ListBuffer[Property]
  val edgeList = new ListBuffer[GraphElement]

  /**
   * Parses a row in Titan's key-value store
   *
   * @return Sequence of graph elements comprising of vertex and adjacent edges
   */
  def parse(): Seq[GraphElement] = {
    val titanRelationFactory = new TitanRelationFactory(vertexId)
    deserializeTitanRow(titanRelationFactory)

    val vertex = titanRelationFactory.createVertex()
    val edgeList = titanRelationFactory.edgeList
    if (vertex.isDefined) {
      titanRelationFactory.edgeList :+ vertex.get
    }
    else {
      edgeList
    }
  }

  /**
   * Deserialize Titan row into vertex properties, and edges in the adjacency list
   *
   * @param titanRelationFactory Relation factory used by Titan to deserialize rows
   */
  private def deserializeTitanRow(titanRelationFactory: TitanRelationFactory) {
    titanRow.serializedEntries.map(entry => {
      titanEdgeSerializer.readRelation(titanRelationFactory, entry, titanTransaction);
      titanRelationFactory.build()
    })
  }
}