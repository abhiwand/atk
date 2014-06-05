package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.driver.spark.titan.reader.{ TitanRow, TitanRowParser }
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.elements.GraphElement
import com.thinkaurelius.titan.diskstorage.util.{ StaticArrayBuffer, StaticByteBuffer }
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry
import com.thinkaurelius.titan.diskstorage.StaticBuffer
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.{ InterruptibleIterator, TaskContext, Partition }
import scala.collection.JavaConversions._
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx

/**
 * RDD that loads Titan graph from HBase.
 *
 * @param hBaseRDD Input RDD
 * @param titanConnector connector to Titan
 */

class TitanHBaseReaderRDD(hBaseRDD: RDD[(ImmutableBytesWritable, Result)],
                          titanConnector: TitanGraphConnector) extends RDD[GraphElement](hBaseRDD) {

  override def getPartitions: Array[Partition] = firstParent[(ImmutableBytesWritable, Result)].partitions

  /**
   * Parses HBase input rows to extract vertices and corresponding edges.
   *
   * @return Iterator of GraphBuilder vertices and edges using GraphBuilder's GraphElement trait
   */
  override def compute(split: Partition, context: TaskContext): Iterator[GraphElement] = {

    val graphElementIterator = new Iterator[GraphElement] {
      val hBaseIterator = firstParent[(ImmutableBytesWritable, Result)].iterator(split, context)
      val titanGraph = titanConnector.connect()
      val titanTransaction = titanGraph.newTransaction(titanGraph.buildTransaction())

      // A single HBase row contains multiple graph elements namely: a single vertex, and adjacent edges
      var rowElementsIterator = Iterator[GraphElement]()

      override def hasNext: Boolean = {
        if (!rowElementsIterator.hasNext) {
          // parsing here instead of instead of in next() method to prevent exceptions
          // that arise when HBase row contains no graph elements
          rowElementsIterator = parseHBaseRow(hBaseIterator, titanGraph, titanTransaction)
        }

        rowElementsIterator.hasNext
      }

      override def next(): GraphElement = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("No more graph elements available.")
        }
        rowElementsIterator.next()
      }

      context.addOnCompleteCallback(() => {
        titanTransaction.commit()
        titanGraph.shutdown()
      })
    }

    new InterruptibleIterator(context, graphElementIterator)
  }

  /**
   * Parse HBase row to extract a single vertex and its adjacent edges
   *
   * This method iterates through HBase rows until it finds a row that contains graph elements.
   *
   * @param hBaseIterator HBase row iterator
   * @param titanGraph Titan graph
   * @param titanTransaction Titan transaction
   *
   * @return Iterator of graph elements for a single row
   */
  private def parseHBaseRow(hBaseIterator: Iterator[(ImmutableBytesWritable, Result)],
                            titanGraph: StandardTitanGraph,
                            titanTransaction: StandardTitanTx): Iterator[GraphElement] = {
    val titanEdgeSerializer = titanGraph.getEdgeSerializer()
    var rowGraphElements = Iterator[GraphElement]()

    // Using while() to skip rows with no graph elements
    while (hBaseIterator.hasNext && !rowGraphElements.hasNext) {
      val hBaseRow = hBaseIterator.next()
      val result = hBaseRow._2
      val rowKey = new StaticByteBuffer(result.getRow)
      val titanRow = getSerializedTitanRow(rowKey, result)

      val titanRowParser = TitanRowParser(titanRow, titanEdgeSerializer, titanTransaction)
      rowGraphElements = titanRowParser.parse().iterator
    }

    rowGraphElements
  }

  /**
   * Get serialized Titan elements from HBase input row
   */
  private def getSerializedTitanRow(rowKey: StaticBuffer, result: Result): TitanRow = {
    val titanColumnFamilyName = com.thinkaurelius.titan.diskstorage.Backend.EDGESTORE_NAME.getBytes();
    val titanColumnFamilyMap = result.getFamilyMap(titanColumnFamilyName);

    val serializedEntries = titanColumnFamilyMap.entrySet().map(entry =>
      StaticBufferEntry.of(new StaticArrayBuffer(entry.getKey), new StaticArrayBuffer(entry.getValue))
    ).toSeq

    new TitanRow(rowKey, serializedEntries)
  }
}
