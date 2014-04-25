//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

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
 * @param append  true to append to an existing graph (incremental graph construction)
 * @param maxVerticesPerCommit Titan performs poorly if you try to commit vertices in too large of batches.
 *                              10k seems to be a pretty we established number to use for Vertices.              
 */
class TitanVertexWriterRDD(prev: RDD[Vertex], 
                           titanConnector: TitanGraphConnector, 
                           val append: Boolean = false, 
                           val maxVerticesPerCommit: Long = 10000L) extends RDD[GbIdToPhysicalId](prev) {

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
      if (count % maxVerticesPerCommit == 0) {
        graph.commit()
      }
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
