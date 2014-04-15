package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.elements.{GbIdToPhysicalId, Vertex}
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.write.VertexWriter
import com.intel.graphbuilder.write.dao.VertexDAO
import com.intel.graphbuilder.write.titan.TitanVertexWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition}

/**
 * RDD that writes to Titan and produces output mapping GbId's to Physical Id's
 * <p>
 * This is an unusual RDD transformation because it has the side effect of writing to Titan.
 * This means extra care is needed to prevent it from being recomputed.
 * </p>
 * @param prev input RDD
 * @param titanConnector connector to Titan
 */
class TitanVertexWriterRDD(prev: RDD[Vertex], titanConnector: TitanGraphConnector, append: Boolean = false) extends RDD[GbIdToPhysicalId](prev) {

  override def getPartitions: Array[Partition] = firstParent[Vertex].partitions

  /**
   * Write to Titan and produce a mapping of GbId's to Physical Id's
   */
  override def compute(split: Partition, context: TaskContext): Iterator[GbIdToPhysicalId] = {

    val graph = titanConnector.connect()
    val writer = new TitanVertexWriter(new VertexWriter(new VertexDAO(graph), append))

    var count = 0L
    val gbIdsToPhyiscalIds = firstParent[Vertex].iterator(split, context).map(v => {
      val id = writer.write(v)
      count += 1
      id
    })

    graph.commit()

    context.addOnCompleteCallback(() => {
      println("vertices written: " + count + " for split: " + split.index)
      graph.shutdown()
    })

    gbIdsToPhyiscalIds
  }
}
