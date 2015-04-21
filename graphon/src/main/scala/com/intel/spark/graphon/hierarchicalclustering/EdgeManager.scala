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
import com.thinkaurelius.titan.core.TitanGraph

/**
 * This is the edge manager class.
 */
object EdgeManager extends Serializable {
  /**
   *
   * @param list a  list of iterable edges. If the list has 2 elements, the head element (an edge) of any of the lists can collapse
   * @return true if the edge can collapse; false otherwise
   */
  def canEdgeCollapse(list: Iterable[VertexOutEdges]): Boolean = {
    (null != list) && (list.toArray.length > 1)
  }

  /**
   * Replace a node with associated meta-node in edges.
   * @param edgeList - a list with the following property:
   *                 head - an edge with the meta node as source node
   *                 tail - a list of edges whose destination nodes will need to be replaced
   * @return the edge list (less the head element) with the destination node replaced by head.source
   */
  def replaceWithMetaNode(edgeList: Iterable[HierarchicalClusteringEdge]): Iterable[HierarchicalClusteringEdge] = {

    if (edgeList.toArray.length > 1) {
      var internalEdge: HierarchicalClusteringEdge = null
      for (edge <- edgeList) {
        if (edge.isInternal) {
          internalEdge = edge
        }
      }
      if (null != internalEdge) {
        for (edge <- edgeList) {
          if (edge != internalEdge) {
            edge.distance = edge.distance * edge.destNodeCount
            edge.dest = internalEdge.src
            edge.destNodeCount = internalEdge.srcNodeCount
          }
        }
      }
    }

    edgeList.filter(e => e.isInternal == false)
  }

  /**
   * Creates a flat list of edges (to be interpreted as outgoing edges) for a meta-node
   * @param list a list of (lists of) edges. The source node of the head element of each list is the metanode
   * @return a flat list of outgoing edges for metanode
   */
  def createOutgoingEdgesForMetaNode(list: Iterable[VertexOutEdges]): (HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge]) = {

    var outgoingEdges: List[HierarchicalClusteringEdge] = List[HierarchicalClusteringEdge]()
    var edge: HierarchicalClusteringEdge = null

    if ((null != list) && (!list.isEmpty)) {
      for (edgeList <- list) {
        if ((null != edgeList) && (null != edgeList.higherDistanceEdgeList)) {
          outgoingEdges = outgoingEdges ++ edgeList.higherDistanceEdgeList.toList
          edge = edgeList.minDistanceEdge
        }
      }
    }

    (edge, outgoingEdges)
  }

  /**
   * Creates 2 internal edges for a collapsed edge
   * @param edge a collapsed edge
   * @return 2 internal edges replacing the collapsed edge in the graph
   */
  def createInternalEdgesForMetaNode(edge: HierarchicalClusteringEdge,
                                     storage: HierarchicalClusteringStorageInterface,
                                     iteration: Int): (Long, Long, List[HierarchicalClusteringEdge]) = {

    var edges: List[HierarchicalClusteringEdge] = List[HierarchicalClusteringEdge]()

    if (null != edge) {
      val metaNodeVertexId = storage.addVertexAndEdges(
        edge.src,
        edge.dest,
        edge.getTotalNodeCount,
        edge.src.toString + "_" + edge.dest.toString,
        iteration)

      edges = edges :+ HierarchicalClusteringEdge(metaNodeVertexId,
        edge.getTotalNodeCount,
        edge.src,
        edge.srcNodeCount,
        HierarchicalClusteringConstants.DefaultNodeCount, true)
      edges = edges :+ HierarchicalClusteringEdge(metaNodeVertexId,
        edge.getTotalNodeCount,
        edge.dest,
        edge.destNodeCount,
        HierarchicalClusteringConstants.DefaultNodeCount, true)

      (metaNodeVertexId, edge.getTotalNodeCount, edges)
    }
    else {
      (HierarchicalClusteringConstants.DefaultVertextId,
        HierarchicalClusteringConstants.DefaultNodeCount,
        edges)
    }

  }

  /**
   * Creates a list of active edges for meta-node
   * @param metaNode
   * @param count
   * @param nonSelectedEdges
   * @return
   */
  def createActiveEdgesForMetaNode(metaNode: Long, count: Long,
                                   nonSelectedEdges: Iterable[HierarchicalClusteringEdge]): List[((Long, Long), HierarchicalClusteringEdge)] = {

    nonSelectedEdges.map(e => ((e.dest, e.destNodeCount),
      HierarchicalClusteringEdge(
        metaNode,
        count,
        e.dest,
        e.destNodeCount,
        e.distance, false))).toList
  }
}
