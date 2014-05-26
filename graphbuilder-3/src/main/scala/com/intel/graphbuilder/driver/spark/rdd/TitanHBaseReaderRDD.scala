package com.intel.graphbuilder.driver.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.commons.configuration.Configuration
import com.intel.graphbuilder.elements.GraphElement
import org.apache.spark.{ TaskContext, Partition }
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.thinkaurelius.titan.diskstorage.util.{ StaticArrayBuffer, StaticByteBuffer }
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry
import scala.collection.mutable.ListBuffer
import com.intel.graphbuilder.driver.spark.titan.reader.{ TitanRow, TitanRowParser }
import scala.collection.JavaConversions._

class TitanHBaseReaderRDD(hBaseRDD: RDD[(ImmutableBytesWritable, Result)], titanConfig: Configuration) extends RDD[GraphElement](hBaseRDD) {

  override def getPartitions: Array[Partition] = firstParent[(ImmutableBytesWritable, Result)].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[GraphElement] = {

    val titanConnector = new TitanGraphConnector(titanConfig)
    val titanGraph = titanConnector.connectToStandardGraph()
    val titanEdgeSerializer = titanGraph.getEdgeSerializer()
    val titanTransaction = titanGraph.newTransaction(titanGraph.buildTransaction())

    var graphElements = ListBuffer[GraphElement]()

    firstParent[(ImmutableBytesWritable, Result)].iterator(split, context).foreach(hBaseRow => {
      val result = hBaseRow._2
      println("result " + result + ", rowkey= " + result.getRow)
      val rowKey = new StaticByteBuffer(result.getRow)

      val titanRow = getSerializedTitanRow(rowKey, result)
      val titanRowParser = TitanRowParser(titanRow, titanEdgeSerializer, titanTransaction)
      val rowGraphElements = titanRowParser.parse()
      println("rowKey " + rowKey + ", size= " + rowGraphElements.size)

      rowGraphElements.foreach(element => graphElements += element)
      // titanTransaction.rollback()
    })

    graphElements.toList.iterator
  }

  private def getSerializedTitanRow(rowKey: StaticByteBuffer, result: Result): TitanRow = {
    val titanColumnFamilyName = com.thinkaurelius.titan.diskstorage.Backend.EDGESTORE_NAME.getBytes();
    val titanColumnFamilyMap = result.getFamilyMap(titanColumnFamilyName);

    val serializedEntries = titanColumnFamilyMap.entrySet().map(entry =>
      StaticBufferEntry.of(new StaticArrayBuffer(entry.getKey), new StaticArrayBuffer(entry.getValue))
    ).toSeq

    new TitanRow(rowKey, serializedEntries)
  }
}
