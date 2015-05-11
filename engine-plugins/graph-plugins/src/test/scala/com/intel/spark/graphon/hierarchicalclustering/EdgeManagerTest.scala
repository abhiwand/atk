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

import org.scalatest.{ FlatSpec, Matchers }

class EdgeManagerTest extends FlatSpec with Matchers {

  val metaNodeId = 10
  val metaNodeCount = 100

  val basicEdgeList: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 1, 2, 1, 1.1f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 1.2f, false)
  )

  val minEdge = HierarchicalClusteringEdge(1, 1, 2, 1, 1.1f, false)
  val nonCollapsableEdgeList: List[VertexOutEdges] = List(
    VertexOutEdges(minEdge, basicEdgeList)
  )
  val outgoingEdgeList: List[VertexOutEdges] = List(
    VertexOutEdges(minEdge, basicEdgeList),
    VertexOutEdges(minEdge, basicEdgeList)
  )

  val metaNodeEdgeList = List(
    HierarchicalClusteringEdge(1, 1, 2, 1, 1.1f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 1.2f, false),
    HierarchicalClusteringEdge(metaNodeId, metaNodeCount, 3, 1, 1.2f, true)
  )

  "edgeManager::createActiveEdgesForMetaNode" should "return valid active edge list" in {
    val result: List[((Long, Long), HierarchicalClusteringEdge)] =
      EdgeManager.createActiveEdgesForMetaNode(metaNodeId, metaNodeCount, basicEdgeList)
    result.foreach(item =>
      {
        val ((id: Long, count: Long), edge: HierarchicalClusteringEdge) = item
        assert(edge.src == metaNodeId)
        assert(edge.srcNodeCount == metaNodeCount)
      })
  }

  "edgeManager::createOutgoingEdgesForMetaNode" should "return valid outgoing edge list" in {
    val (edge, outgoingEdges): (HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge]) =
      EdgeManager.createOutgoingEdgesForMetaNode(outgoingEdgeList)

    assert(edge == minEdge)
    assert(outgoingEdges.size == outgoingEdgeList.size * basicEdgeList.size)
  }

  "edgeManager::replaceWithMetaNode" should "return the input edge list if no internal edge is present" in {
    val edges: Iterable[HierarchicalClusteringEdge] =
      EdgeManager.replaceWithMetaNode(basicEdgeList)

    assert(edges == basicEdgeList)
  }

  "edgeManager::replaceWithMetaNode" should "return valid edge list with meta-node inserted" in {
    val edges: Iterable[HierarchicalClusteringEdge] =
      EdgeManager.replaceWithMetaNode(metaNodeEdgeList)

    assert(edges.size == 2)
    edges.foreach(item =>
      {
        assert(item.dest == metaNodeId)
        assert(item.destNodeCount == metaNodeCount)
        assert(item.isInternal == false)
      })
  }

  "edgeManager::canEdgeCollapse" should "return true" in {
    assert(EdgeManager.canEdgeCollapse(outgoingEdgeList))
  }

  "edgeManager::canEdgeCollapse" should "return false" in {
    assert(EdgeManager.canEdgeCollapse(nonCollapsableEdgeList) == false)
  }
}
