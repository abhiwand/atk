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

package org.apache.spark.graphx.lib.ia.plugins

import org.apache.spark.graphx._
import org.apache.spark.util.collection.OpenHashSet

import scala.reflect.ClassTag

/**
 * Compute the local clustering coefficient of each vertex in a graph.
 *
 *
 * Note that the input graph should have its edges in canonical direction
 * (i.e. the `sourceId` less than `destId`). Also the graph must have been partitioned
 * using [[org.apache.spark.graphx.Graph#partitionBy]].
 *
 * PERFORMANCE/COMPATIBILITY NOTE:  This routine was written to the GraphX API in Spark 1.1
 * Going forward, an upgrade to the Spark 1.2 GraphX API could yield performance gains.
 */
object ClusteringCoefficient {

  type VertexSet = OpenHashSet[Long]

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): (Graph[Double, ED], Double) = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[VertexSet] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(4)
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          if (nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }
    // join the sets with the graph
    val setGraph: Graph[VertexSet, ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }
    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(et: EdgeTriplet[VertexSet, ED]): Iterator[(VertexId, Int)] = {
      assert(et.srcAttr != null, "GraphX, clustering coefficient, edgeFunc: edge source has null adjacency list.")
      assert(et.dstAttr != null, "GraphX, clustering coefficient, edgeFunc: edge source has null adjacency list.")
      val (smallSet, largeSet) = if (et.srcAttr.size < et.dstAttr.size) {
        (et.srcAttr, et.dstAttr)
      }
      else {
        (et.dstAttr, et.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != et.srcId && vid != et.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      Iterator((et.srcId, counter), (et.dstId, counter))
    }
    // compute the intersection along edges

    val triangleDoubleCounts: VertexRDD[Int] = setGraph.mapReduceTriplets(edgeFunc, _ + _)

    val degreesChooseTwo: Graph[Long, ED] = setGraph.mapVertices({ case (vid, vertexSet) => (chooseTwo(vertexSet.size)) })

    val doubleCountOfTriangles: Long =
      triangleDoubleCounts.aggregate[Long](0L)({ case (x: Long, (vid: VertexId, triangleDoubleCount: Int)) => x + triangleDoubleCount.toLong }, _ + _)

    val totalDegreesChooseTwo: Long =
      degreesChooseTwo.vertices.aggregate[Long](0L)({ case (x: Long, (vid: VertexId, degreeChoose2: Long)) => x + degreeChoose2 }, _ + _)

    val globalClusteringCoefficient = if (totalDegreesChooseTwo > 0) {
      (doubleCountOfTriangles / 2.0d) / totalDegreesChooseTwo.toDouble
    }
    else {
      0.0d
    }

    val localClusteringCoefficientGraph = degreesChooseTwo.outerJoinVertices(triangleDoubleCounts) {
      (vid, degreeChoose2, optCounter: Option[Int]) =>
        if (degreeChoose2 == 0L) {
          0.0d
        }
        else {
          val triangleDoubleCount = optCounter.getOrElse(0)
          (triangleDoubleCount.toDouble / 2.0d) / degreeChoose2.toDouble
        }
    }

    (localClusteringCoefficientGraph, globalClusteringCoefficient)

  }

  private def chooseTwo(n: Long): Long = (n * (n - 1)) / 2

}