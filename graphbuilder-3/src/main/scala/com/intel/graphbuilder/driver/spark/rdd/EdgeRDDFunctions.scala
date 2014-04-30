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

import com.intel.graphbuilder.elements._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.write.EdgeWriter
import com.intel.graphbuilder.write.dao.{VertexDAO, EdgeDAO}
import org.apache.spark.SparkContext._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

/**
 * Functions that are applicable to Edge RDD's
 *
 * This is best used by importing GraphBuilderRDDImplicits._
 *
 * @param self input that these functions are applicable to
 * @param maxEdgesPerCommit Titan performs poorly if you try to commit edges in too
 *                          large of batches.  With Vertices the limit is much lower (10k).
 *                          The limit for Edges is much higher but there is still a limit.
 *                          It is hard to tell what the right number is for this one.
 *                          I think somewhere larger than 400k is getting too big.
 */
class EdgeRDDFunctions(self: RDD[Edge], val maxEdgesPerCommit: Long = 50000L) extends Serializable {

  /**
   * Merge duplicate Edges, creating a new Edge that has a combined set of properties.
   *
   * @return an RDD without duplicates
   */
  def mergeDuplicates(): RDD[Edge] = {
    self.groupBy(m => m.id).mapValues(dups => dups.reduce((m1, m2) => m1.merge(m2))).values
  }

  /**
   * For every Edge, create an additional Edge going in the opposite direction. This
   * method will double the size of the RDD.
   * <p>
   * Depending on the source data, duplicates may be created in this process. So you may
   * need to merge duplicates after.
   * </p>
   */
  def biDirectional(): RDD[Edge] = {
    self.flatMap(edge => List(edge, edge.reverse()))
  }

  /**
   * For every Edge, create two vertices, one from the tail Vertex GbId, and one from the
   * head Vertex GbId.
   * <p>
   * This functionality was called "retain dangling edges" in GB version 2.
   * </p>
   */
  def verticesFromEdges(): RDD[Vertex] = {
    self.flatMap(edge => List(new Vertex(edge.tailVertexGbId, Nil), new Vertex(edge.headVertexGbId, Nil)))
  }

  /**
   * Join Edges with the Physical Id's so they won't need to be looked up when writing Edges.
   *
   * This is an inner join.  Edges that can't be joined are dropped.
   */
  def joinWithPhysicalIds(ids: RDD[GbIdToPhysicalId]): RDD[Edge] = {

    val idsByGbId = ids.groupBy(idMap => idMap.gbId)

    // set physical ids for tail vertices
    val edgesByTail = self.groupBy(edge => edge.tailVertexGbId)
    val edgesWithTail = idsByGbId.join(edgesByTail).flatMapValues(value => {
      val gbIdToPhysicalIds = value._1
      val physicalId = gbIdToPhysicalIds(0).physicalId
      val edges = value._2
      edges.map(e => e.copy(tailPhysicalId = physicalId))
    }).values

    // set physical ids for head vertices
    val edgesByHead = edgesWithTail.groupBy(e => e.headVertexGbId)
    val edgesWithPhysicalIds = idsByGbId.join(edgesByHead).flatMapValues(value => {
      val gbIdToPhysicalIds = value._1
      val physicalId = gbIdToPhysicalIds(0).physicalId
      val edges = value._2
      edges.map(e => e.copy(headPhysicalId = physicalId))
    }).values

    edgesWithPhysicalIds
  }

  /**
   * Filter Edges that do NOT have physical ids
   */
  def filterEdgesWithoutPhysicalIds(): RDD[Edge] = {
    self.filter(edge => {
      if (edge.tailPhysicalId == null || edge.headPhysicalId == null) false
      else true
    })
  }

  /**
   * Write the Edges to Titan using the supplied connector
   * @param append true to append to an existing graph
   */
  def write(titanConnector: TitanGraphConnector, append: Boolean): Unit = {

    self.context.runJob(self, (context: TaskContext, iterator: Iterator[Edge]) => {
      val graph = titanConnector.connect()
      val edgeDAO = new EdgeDAO(graph, new VertexDAO(graph))
      val writer = new EdgeWriter(edgeDAO, append)

      try {
        var count = 0L
        while (iterator.hasNext) {
          writer.write(iterator.next())
          count += 1
          if (count % maxEdgesPerCommit == 0) {
            graph.commit()
          }
        }

        println("wrote edges: " + count + " for split: " + context.partitionId)

        graph.commit()
      }
      finally {
        graph.shutdown()
      }
    })
  }

  /**
   * Write the Edges to Titan using the supplied connector
   * @param append true to append to an existing graph
   * @param gbIdToPhysicalIdMap see GraphBuilderConfig.broadcastVertexIds
   */
  def write(titanConnector: TitanGraphConnector, gbIdToPhysicalIdMap: Broadcast[Map[Property, AnyRef]], append: Boolean): Unit = {

    self.context.runJob(self, (context: TaskContext, iterator: Iterator[Edge]) => {
      val graph = titanConnector.connect()
      val edgeDAO = new EdgeDAO(graph, new VertexDAO(graph))
      val writer = new EdgeWriter(edgeDAO, append)

      try {
        var count = 0L
        while (iterator.hasNext) {
          val edge = iterator.next()
          edge.tailPhysicalId = gbIdToPhysicalIdMap.value(edge.tailVertexGbId)
          edge.headPhysicalId = gbIdToPhysicalIdMap.value(edge.headVertexGbId)
          writer.write(edge)
          count += 1
          if (count % maxEdgesPerCommit == 0) {
            graph.commit()
          }
        }

        println("wrote edges: " + count + " for split: " + context.partitionId)

        graph.commit()
      }
      finally {
        graph.shutdown()
      }
    })
  }
}
