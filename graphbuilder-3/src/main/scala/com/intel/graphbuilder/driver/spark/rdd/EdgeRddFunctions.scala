//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import com.intel.graphbuilder.driver.spark.titan.JoinBroadcastVariable
import com.intel.graphbuilder.elements._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.write.EdgeWriter
import com.intel.graphbuilder.write.dao.{ EdgeDAO, VertexDAO }
import org.apache.spark.SparkContext._
import org.apache.spark.{ RangePartitioner, TaskContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

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
class EdgeRddFunctions(self: RDD[GBEdge], val maxEdgesPerCommit: Long = 10000L) extends Serializable {

  /**
   * Merge duplicate Edges, creating a new Edge that has a combined set of properties.
   *
   * @return an RDD without duplicates
   */
  def mergeDuplicates(): RDD[GBEdge] = {
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
  def biDirectional(): RDD[GBEdge] = {
    self.flatMap(edge => List(edge, edge.reverse()))
  }

  /**
   * For every Edge, output its head Vertex GbId
   */
  def headVerticesGbIds(): RDD[Property] = {
    self.map(edge => edge.headVertexGbId)
  }

  /**
   * For every Edge, output its tail Vertex GbId
   */
  def tailVerticesGbIds(): RDD[Property] = {
    self.map(edge => edge.tailVertexGbId)
  }

  /**
   * For every Edge, create two vertices, one from the tail Vertex GbId, and one from the
   * head Vertex GbId.
   * <p>
   * This functionality was called "retain dangling edges" in GB version 2.
   * </p>
   */
  def verticesFromEdges(): RDD[GBVertex] = {
    self.flatMap(edge => List(new GBVertex(edge.tailVertexGbId, Set.empty[Property]), new GBVertex(edge.headVertexGbId, Set.empty[Property])))
  }

  /**
   * Join Edges with the Physical Id's so they won't need to be looked up when writing Edges.
   *
   * This is an inner join.  Edges that can't be joined are dropped.
   */
  def joinWithPhysicalIds(ids: RDD[GbIdToPhysicalId]): RDD[GBEdge] = {

    val idsByGbId = ids.map(idMap => (idMap.gbId, idMap))

    // set physical ids for tail (source) vertices
    val edgesByTail = self.map(edge => (edge.tailVertexGbId, edge))

    // Range-partitioner helps alleviate the "com.esotericsoftware.kryo.KryoException: java.lang.NegativeArraySizeException"
    // which occurs when we spill blocks to disk larger than 2GB but does not completely fix the problem
    // TODO: Find a better way to handle supernodes: Look at skewed joins in Pig
    implicit val propertyOrdering = PropertyOrdering // Ordering is needed by Spark's range partitioner
    val tailPartitioner = new RangePartitioner(edgesByTail.partitions.length, edgesByTail)
    val edgesWithTail = edgesByTail.join(idsByGbId, tailPartitioner).map {
      case (gbId, (edge, gbIdToPhysicalId)) =>
        val physicalId = gbIdToPhysicalId.physicalId
        (edge.headVertexGbId, edge.copy(tailPhysicalId = physicalId))
    }

    // set physical ids for head (destination) vertices
    val edgesWithPhysicalIds = edgesWithTail.join(idsByGbId).map {
      case (gbId, (edge, gbIdToPhysicalId)) => {
        val physicalId = gbIdToPhysicalId.physicalId
        edge.copy(headPhysicalId = physicalId)
      }
    }

    edgesWithPhysicalIds
  }

  /**
   * Filter Edges that do NOT have physical ids
   */
  def filterEdgesWithoutPhysicalIds(): RDD[GBEdge] = {
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

    self.context.runJob(self, (context: TaskContext, iterator: Iterator[GBEdge]) => {

      EnvironmentValidator.validateISparkDepsAvailable

      val graph = titanConnector.connect() //TitanGraphConnector.getGraphFromCache(titanConnector)
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
        //Do not shut down graph when using cache since graph instances are automatically shutdown when
        //no more references are held
        graph.shutdown()
      }
    })
  }

  /**
   * Write the Edges to Titan using the supplied connector
   * @param append true to append to an existing graph
   * @param gbIdToPhysicalIdMap see GraphBuilderConfig.broadcastVertexIds
   */
  def write(titanConnector: TitanGraphConnector, gbIdToPhysicalIdMap: JoinBroadcastVariable[Property, AnyRef], append: Boolean): Unit = {

    self.context.runJob(self, (context: TaskContext, iterator: Iterator[GBEdge]) => {
      val graph = titanConnector.connect() //TitanGraphConnector.getGraphFromCache(titanConnector)
      val edgeDAO = new EdgeDAO(graph, new VertexDAO(graph))
      val writer = new EdgeWriter(edgeDAO, append)

      try {
        var count = 0L
        while (iterator.hasNext) {
          val edge = iterator.next()
          edge.tailPhysicalId = gbIdToPhysicalIdMap(edge.tailVertexGbId)
          edge.headPhysicalId = gbIdToPhysicalIdMap(edge.headVertexGbId)
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
        //Do not shut down graph when using cache since graph instances are automatically shutdown when
        //no more references are held
        graph.shutdown()
      }
    })
  }
}
