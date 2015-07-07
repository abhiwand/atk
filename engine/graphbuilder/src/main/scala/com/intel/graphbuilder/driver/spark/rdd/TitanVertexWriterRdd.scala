/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.graphbuilder.driver.spark.rdd

import com.intel.taproot.graphbuilder.elements.{ GbIdToPhysicalId, GBVertex }
import com.intel.taproot.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.taproot.graphbuilder.write.VertexWriter
import com.intel.taproot.graphbuilder.write.dao.VertexDAO
import com.intel.taproot.graphbuilder.write.titan.TitanVertexWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, TaskContext }

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
class TitanVertexWriterRdd(prev: RDD[GBVertex],
                           titanConnector: TitanGraphConnector,
                           val append: Boolean = false,
                           val maxVerticesPerCommit: Long = 10000L) extends RDD[GbIdToPhysicalId](prev) {

  override def getPartitions: Array[Partition] = firstParent[GBVertex].partitions

  /**
   * Write to Titan and produce a mapping of GbId's to Physical Id's
   */
  override def compute(split: Partition, context: TaskContext): Iterator[GbIdToPhysicalId] = {

    val graph = TitanGraphConnector.getGraphFromCache(titanConnector)
    val writer = new TitanVertexWriter(new VertexWriter(new VertexDAO(graph), append))

    var count = 0L
    val gbIdsToPhyiscalIds = firstParent[GBVertex].iterator(split, context).map(v => {
      val id = writer.write(v)
      count += 1
      if (count % maxVerticesPerCommit == 0) {
        graph.commit()
      }
      id
    })

    graph.commit()

    context.addTaskCompletionListener(context => {
      println("vertices written: " + count + " for split: " + split.index)
      //Do not shut down graph when using cache since graph instances are automatically shutdown when
      //no more references are held
      //graph.shutdown()
    })

    gbIdsToPhyiscalIds
  }
}
