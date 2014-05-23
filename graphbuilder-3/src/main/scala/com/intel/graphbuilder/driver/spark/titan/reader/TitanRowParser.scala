package com.intel.graphbuilder.driver.spark.titan.reader

import com.thinkaurelius.titan.graphdb.database.EdgeSerializer
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler
import scala.collection.mutable.ListBuffer
import com.intel.graphbuilder.elements.{ GraphElement, Property }

case class TitanRowParser(titanRow: TitanRow, titanEdgeSerializer: EdgeSerializer, titanTransaction: StandardTitanTx) {

  val vertexId = IDHandler.getKeyID(titanRow.rowKey)

  val vertexProperties = new ListBuffer[Property]
  val edgeList = new ListBuffer[GraphElement]

  def parse(): Seq[GraphElement] = {

    val titanRelationFactory = new TitanRelationFactory(vertexId)
    deserializeTitanRow(titanRelationFactory)

    val vertex = titanRelationFactory.createVertex()
    val edgeList = titanRelationFactory.edgeList
    val graphElements: Seq[GraphElement] = edgeList :+ vertex
    graphElements
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