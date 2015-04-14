package com.intel.spark.graphon.hierarchicalclustering

import org.scalatest.{ Matchers, FlatSpec }

class EdgeDistanceMinTest extends FlatSpec with Matchers {

  trait EdgeDistanceMinClassTest {

    val nullEdgeList = null

    val emptyEdgeList = List()

    val basicEdgeList: List[HierarchicalClusteringEdge] = List(
      HierarchicalClusteringEdge(1,1,2,1,1.1f,false),
      HierarchicalClusteringEdge(2,1,3,1,1.2f,false)
    )

    val reversedEdgeList: List[HierarchicalClusteringEdge] = List(
      HierarchicalClusteringEdge(1,1,2,1,1.1f,false),
      HierarchicalClusteringEdge(2,1,1,1,1.1f,false),
      HierarchicalClusteringEdge(2,1,3,1,1.2f,false)
    )
  }

  "edgeDistance::min" should "null" in new EdgeDistanceMinClassTest {
    val (minEdge, list) = EdgeDistance.min(nullEdgeList)
    assert(minEdge == null)
    assert(list == VertexOutEdges(null,null))

  }

  "edgeDistance::min" should "empty" in new EdgeDistanceMinClassTest {
    val (minEdge, list) = EdgeDistance.min(emptyEdgeList)
    assert(minEdge == null)
    assert(list == VertexOutEdges(null,null))

  }

  "edgeDistance::min" should "basic" in new EdgeDistanceMinClassTest {
    val (minEdge, list) = EdgeDistance.min(basicEdgeList)
    assert(minEdge.src == 1)
    assert(minEdge.dest == 2)

  }

  "edgeDistance::min" should "reversed" in new EdgeDistanceMinClassTest {
    val (minEdge, list) = EdgeDistance.min(reversedEdgeList)
    assert(minEdge.src == 1)
    assert(minEdge.dest == 2)
  }
}
