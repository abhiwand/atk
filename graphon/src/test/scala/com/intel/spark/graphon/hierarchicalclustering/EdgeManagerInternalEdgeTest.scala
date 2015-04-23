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

import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }

class EdgeManagerInternalEdgeTest extends FlatSpec with Matchers with MockitoSugar {

  val mockNodeId = 100
  val minEdge = HierarchicalClusteringEdge(1, 1, 2, 1, 1.1f, false)

  val mockInstance = mock[HierarchicalClusteringStorageInterface]
  when(mockInstance.addVertexAndEdges(1, 2, 2, "1_2", 1)).thenReturn(mockNodeId)

  "edgeManager::createInternalEdgesForMetaNode" should "return default internal edges on null" in {
    val (metanode, metanodeCount, metaEdges) = EdgeManager.createInternalEdgesForMetaNode(null, mockInstance, 1)

    assert(HierarchicalClusteringConstants.DefaultVertextId == metanode)
    assert(HierarchicalClusteringConstants.DefaultNodeCount == metanodeCount)
    assert(metaEdges.size == 0)
  }

  "edgeManager::createInternalEdgesForMetaNode" should "return valid internal edge list" in {

    val (metanode, metanodeCount, metaEdges) = EdgeManager.createInternalEdgesForMetaNode(minEdge, mockInstance, 1)

    assert(metanode == mockNodeId)
    assert(metanodeCount == 2)
    assert(metaEdges.size == 2)

    metaEdges.foreach(edge =>
      {
        assert(edge.src == mockNodeId)
        assert(edge.srcNodeCount == 2)
        assert(edge.isInternal)
      })
  }
}
