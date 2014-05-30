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
import org.apache.spark.{ TaskContext, Partition }
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

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
   * Parses HBase input rows to extract vertices and corresponding edges
   *
   * @return Iterator with GraphBuilder vertices and edges
   */
  override def compute(split: Partition, context: TaskContext): Iterator[GraphElement] = {

    val titanGraph = titanConnector.connect()
    val titanEdgeSerializer = titanGraph.getEdgeSerializer()
    val titanTransaction = titanGraph.newTransaction(titanGraph.buildTransaction())

    var graphElements = ListBuffer[GraphElement]()

    firstParent[(ImmutableBytesWritable, Result)].iterator(split, context).foreach(hBaseRow => {
      val result = hBaseRow._2
      val rowKey = new StaticByteBuffer(result.getRow)
      val titanRow = getSerializedTitanRow(rowKey, result)

      val titanRowParser = TitanRowParser(titanRow, titanEdgeSerializer, titanTransaction)
      val rowGraphElements = titanRowParser.parse()

      graphElements ++= rowGraphElements

    })

    context.addOnCompleteCallback(() => {
      titanTransaction.rollback()
      titanGraph.shutdown()
    })

    graphElements.toList.iterator
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
