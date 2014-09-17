package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.elements.GraphElement
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.TitanConverter
import com.thinkaurelius.titan.hadoop.FaunusVertex
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{ InterruptibleIterator, Partition, TaskContext }

/**
 * RDD that loads Titan graph from HBase.
 *
 * @param hBaseRDD Input RDD
 * @param titanConnector connector to Titan
 */

class TitanHBaseReaderRDD(hBaseRDD: RDD[(NullWritable, FaunusVertex)],
                          titanConnector: TitanGraphConnector) extends RDD[GraphElement](hBaseRDD) {

  override def getPartitions: Array[Partition] = firstParent[(NullWritable, FaunusVertex)].partitions

  /**
   * Parses HBase input rows to extract vertices and corresponding edges.
   *
   * @return Iterator of GraphBuilder vertices and edges using GraphBuilder's GraphElement trait
   */
  override def compute(split: Partition, context: TaskContext): Iterator[GraphElement] = {

    val graphElements = firstParent[(NullWritable, FaunusVertex)].iterator(split, context).flatMap(hBaseRow => {
      val faunusVertex = hBaseRow._2

      val gbVertex = TitanConverter.toGraphBuilderVertex(faunusVertex)
      val gbEdges = TitanConverter.toGraphBuilderEdges(faunusVertex)

      val rowGraphElements: Iterator[GraphElement] = Iterator(gbVertex) ++ gbEdges

      rowGraphElements
    })

    new InterruptibleIterator(context, graphElements)
  }

}
