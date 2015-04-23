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

package com.intel.spark.graphon.hierarchicalclustering

import java.io.Serializable

/**
 * This is the edge distance class.
 */
object EdgeDistance extends Serializable {

  /**
   * Calculates the minimum distance of an edge list
   * @param edgeList a list of active edges
   * @return a vertex distance class (vertex id, min distance edge, non-min distance edges)
   */
  def min(edgeList: Iterable[HierarchicalClusteringEdge]): (HierarchicalClusteringEdge, VertexOutEdges) = {

    var dist: Float = Int.MaxValue
    var edgeWithMinDist: HierarchicalClusteringEdge = null
    var nonMinDistEdges: List[HierarchicalClusteringEdge] = List[HierarchicalClusteringEdge]()

    if ((null != edgeList) && (!edgeList.isEmpty)) {
      for (edge <- edgeList) {
        if (null != edge) {
          if (edge.distance < dist) {

            // found a smaller distance edge.
            // save it in edgeWithMinDist & adjust the overall min distance
            dist = edge.distance
            if (edgeWithMinDist != null) {
              nonMinDistEdges = nonMinDistEdges :+ edgeWithMinDist
            }
            edgeWithMinDist = edge
          }
          else if (edge.distance == dist) {
            if (edgeWithMinDist != null) {
              if (edge.src.toString < edgeWithMinDist.src.toString) {

                // found an equal distance edge but with node id smaller.
                // save it in edgeWithMinDist
                nonMinDistEdges = nonMinDistEdges :+ edgeWithMinDist
                edgeWithMinDist = edge
              }
              else {

                // found equal distance edge but with node id higher. Add it to the list of non-selected
                nonMinDistEdges = nonMinDistEdges :+ edge
              }
            }
            else {

              // rare scenario. Found a small distance edge but edgeWithMinDist is not set.
              // set it.
              edgeWithMinDist = edge
            }
          }
          else {

            // found bigger distance edge. Add it to the list of non-selected.
            nonMinDistEdges = nonMinDistEdges :+ edge
          }
        }
      }

      if (null != edgeWithMinDist) {

        // edgeWithMinDist can be null in rare cases. We need to test for null
        if (edgeWithMinDist.dest < edgeWithMinDist.src) {

          // swap the node ids so the smaller node is always source
          swapEdgeInfo(edgeWithMinDist)
        }

        (edgeWithMinDist, VertexOutEdges(edgeWithMinDist, nonMinDistEdges))
      }
      else {
        (null, VertexOutEdges(null, null))
      }
    }
    else {
      (null, VertexOutEdges(null, null))
    }
  }

  /**
   * Sum (edgeDistance * SourceNodeWeight) / Sum (SourceNodeWeight)
   *
   * @param edges a list of active edges
   * @return the average distance, as per formula above
   */
  def weightedAvg(edges: Iterable[HierarchicalClusteringEdge]): Float = {
    var dist: Float = 0
    var nodeCount: Long = 0

    for (e <- edges) {
      dist += (e.distance * e.srcNodeCount)
      nodeCount += e.srcNodeCount
    }

    if (nodeCount > 0) {
      dist = dist / nodeCount
    }

    dist
  }

  /**
   * Sum (edgeDistance) / (Total edges in the Iterable)
   *
   * @param edges a list of active edges
   * @return the head of the input list with the distance adjusted as per formula
   */
  def simpleAvg(edges: Iterable[HierarchicalClusteringEdge], swapInfo: Boolean): HierarchicalClusteringEdge = {
    var dist: Float = 0
    var edgeCount = 0

    for (e <- edges) {
      dist += e.distance
      edgeCount += 1
    }

    if (edgeCount > 1) {
      val head = edges.head
      head.distance = dist / edgeCount

      if (swapInfo) {
        swapEdgeInfo(head)
      }
    }

    if (!edges.isEmpty) {
      edges.head
    }
    else {
      null
    }

  }

  def swapEdgeInfo(edge: HierarchicalClusteringEdge): Unit = {

    val tmpName = edge.src
    val tmpNodeCount = edge.srcNodeCount

    edge.src = edge.dest
    edge.dest = tmpName
    edge.srcNodeCount = edge.destNodeCount
    edge.destNodeCount = tmpNodeCount
  }
}
