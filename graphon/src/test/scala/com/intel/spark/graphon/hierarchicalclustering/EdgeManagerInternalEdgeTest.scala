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
