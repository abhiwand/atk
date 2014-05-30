
package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.elements.GraphElement
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler
import com.thinkaurelius.titan.diskstorage.StaticBuffer

/**
 * Parses a row in Titan's key-value store. Each row represents a vertex and its adjacent edges
 *
 * @param titanRow Serialized Titan row
 * @param titanEdgeSerializer Titan's serializer/deserializer
 * @param titanTransaction Titan transaction
 */
case class TitanRowParser(titanRow: TitanRow, titanEdgeSerializer: EdgeSerializer, titanTransaction: StandardTitanTx) {

  // Physical ID for Titan vertex
  private val vertexId = getTitanVertexID(titanRow.rowKey)

  /**
   * Parses a row in Titan's key-value store.
   *
   * @return Sequence of graph elements comprising of vertex and adjacent edges
   */
  def parse(): Seq[GraphElement] = {
    val titanRelationFactory = new TitanRelationFactory(vertexId)

    try {
      deserializeTitanRow(titanRelationFactory)
      titanRelationFactory.createGraphElements()
    }
    catch {
      case e: Exception => {
        throw new RuntimeException("Unable to parse Titan row:" + titanRow, e)
      }
    }
  }

  /**
   * Get the unique Physical ID for the Vertex from the row key.
   */
  private def getTitanVertexID(rowKey: StaticBuffer) = {
    try {
      IDHandler.getKeyID(titanRow.rowKey)
    }
    catch {
      case e: Exception => {
        throw new RuntimeException("Unable to extract Titan row key:" + rowKey, e)
      }
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

