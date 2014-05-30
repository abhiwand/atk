package com.intel.graphbuilder.testutils

import com.thinkaurelius.titan.graphdb.database.{StandardTitanGraph, EdgeSerializer}
import com.thinkaurelius.titan.core.{TitanProperty, TitanElement, TitanVertex}
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry
import com.thinkaurelius.titan.graphdb.internal.InternalRelation
import com.intel.graphbuilder.driver.spark.titan.reader.TitanRow
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler
import com.intel.graphbuilder.elements.Property
import com.thinkaurelius.titan.diskstorage.StaticBuffer
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._


/**
 * Utility methods for creating test data for reading Titan graphs.
 *
 * These utilities serialize Titan graph elements into the format that Titan uses for its key-value stores.
 * The utilities also create GraphBuilder elements from Titan elements.
 */
object TitanReaderUtils {

  /**
   * Create GraphBuilder properties from a list of Titan properties.
   *
   * @param properties Titan properties
   * @return Iterable of GraphBuilder properties
   */
  def createGbProperties(properties: Iterable[TitanProperty]): Seq[Property] = {
    properties.map(p => Property(p.getPropertyKey().getName(), p.getValue())).toList
  }

  /**
   * Create serialized representation of the vertices and edges in the Titan graph using
   * Titan's edge serializer.
   *
   * Each serialized row represents a vertex and its adjacency list.
   *
   * @param graph Titan graph
   * @return Map of vertex name, and corresponding serialized Titan row
   */
  def createTestTitanRows(graph: StandardTitanGraph): Map[String, TitanRow] = {
    val edgeSerializer = graph.getEdgeSerializer()
    var entryMap = HashMap[String, TitanRow]()

    graph.getVertices().foreach(vertex => {
      val titanVertex = vertex.asInstanceOf[TitanVertex]
      val entryList = ListBuffer[Entry]()

      titanVertex.getEdges().foreach(edge => {
        entryList ++= serializeGraphElement(edgeSerializer, titanVertex, edge.asInstanceOf[TitanElement])
      })

      titanVertex.getProperties().foreach(property => {
        entryList ++= serializeGraphElement(edgeSerializer, titanVertex, property.asInstanceOf[TitanElement])
      })

      val titanRow = new TitanRow(IDHandler.getKey(titanVertex.getID), entryList.toSeq)
      entryMap += (vertex.getProperty("name").toString() -> titanRow)
    })

    entryMap
  }

  /**
   * Serialize a single vertex property or edge using Titan's edge serializer.
   *
   * @param titanEdgeSerializer Titan edge serializer
   * @param titanVertex Titan vertex
   * @param titanElement Titan vertex property or edge
   * @return
   */
  def serializeGraphElement(titanEdgeSerializer: EdgeSerializer, titanVertex: TitanVertex, titanElement: TitanElement): Seq[Entry] = {
    val relation = titanElement.asInstanceOf[InternalRelation]

    val entryList = ListBuffer[Entry]()

    for (pos <- 0 until relation.getLen()) {
      if (relation.getVertex(pos) == titanVertex) { // Ensure that we are serializing properties for the right vertex
        entryList += titanEdgeSerializer.writeRelation(relation, pos, relation.tx())
      }
    }
    entryList.toSeq
  }

  /**
   * Create HBase rows from the serialized Titan rows.
   *
   * @param titanRowMap Map of Titan rows
   * @return Map of HBase rows
   */
  def createTestHBaseRows(titanRowMap: Map[String, TitanRow]): Map[org.apache.hadoop.hbase.io.ImmutableBytesWritable,
    org.apache.hadoop.hbase.client.Result] = {

    titanRowMap.map(row => {
      val titanRow: TitanRow = row._2
      val rowKey = titanRow.rowKey.as[Array[Byte]](StaticBuffer.ARRAY_FACTORY)
      val entries = titanRow.serializedEntries
      val titanColumnFamilyName = com.thinkaurelius.titan.diskstorage.Backend.EDGESTORE_NAME.getBytes()
      val dummyTimestamp = 1
      val dummyType = 0.toByte

      val hBaseCells = entries.map(entry => {
        CellUtil.createCell(rowKey, titanColumnFamilyName, entry.getArrayColumn(), dummyTimestamp, dummyType, entry.getArrayValue)
      })

      (new ImmutableBytesWritable(rowKey), Result.create(hBaseCells))

    })
  }
}
