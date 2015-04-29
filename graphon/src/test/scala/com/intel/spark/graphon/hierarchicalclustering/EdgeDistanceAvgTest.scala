package com.intel.spark.graphon.hierarchicalclustering

import org.scalatest.{ FlatSpec, Matchers }

class EdgeDistanceAvgTest extends FlatSpec with Matchers {

  val emptyEdgeList = List()

  val basicEdgeList: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 1, 2, 1, 1.1f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 1.3f, false)
  )

  val reversedEdgeList: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 4, 2, 1, 1.0f, false),
    HierarchicalClusteringEdge(2, 1, 1, 1, 1.4f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 1.2f, false)
  )

  "edgeDistance::weightedAvg" should "be 0 for empty lists" in {
    val dist = EdgeDistance.weightedAvg(emptyEdgeList)
    assert(dist == 0)
  }

  "edgeDistance::weightedAvg" should "be non 0" in {
    val dist = EdgeDistance.weightedAvg(basicEdgeList)
    assert(dist == 1.2f)
  }

  "edgeDistance::weightedAvg" should "return non 0 value" in {
    val dist = EdgeDistance.weightedAvg(reversedEdgeList)
    assert(dist == 1.1f)
  }

  "edgeDistance::simpleAvg" should "be 0 for empty lists" in {
    val edge = EdgeDistance.simpleAvg(emptyEdgeList, false)
    assert(edge == null)
  }

  "edgeDistance::simpleAvg" should "be non 0" in {
    val edge = EdgeDistance.simpleAvg(basicEdgeList, false)
    assert(edge.distance == 1.2f)
  }

  "edgeDistance::simpleAvg" should "return non 0 value" in {
    val edge = EdgeDistance.simpleAvg(reversedEdgeList, false)
    assert(edge.distance == 1.2f)
  }
}
