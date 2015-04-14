package com.intel.spark.graphon.hierarchicalclustering

import org.scalatest.{FlatSpec, Matchers}

class EdgeDistanceAvgTest extends FlatSpec with Matchers {

  trait EdgeDistanceAvgClassTest {

    val emptyEdgeList = List()

    val basicEdgeList: List[HierarchicalClusteringEdge] = List(
      HierarchicalClusteringEdge(1,1,2,1,1.1f,false),
      HierarchicalClusteringEdge(2,1,3,1,1.3f,false)
    )

    val reversedEdgeList: List[HierarchicalClusteringEdge] = List(
      HierarchicalClusteringEdge(1,4,2,1,1.0f,false),
      HierarchicalClusteringEdge(2,1,1,1,1.4f,false),
      HierarchicalClusteringEdge(2,1,3,1,1.2f,false)
    )
  }

  "edgeDistance::weightedAvg" should "empty" in new EdgeDistanceAvgClassTest {
    val dist = EdgeDistance.weightedAvg(emptyEdgeList)
    assert(dist == 0)

  }

  "edgeDistance::weightedAvg" should "basic" in new EdgeDistanceAvgClassTest {
    val dist = EdgeDistance.weightedAvg(basicEdgeList)
    assert(dist == 1.2f)

  }

  "edgeDistance::weightedAvg" should "reversed" in new EdgeDistanceAvgClassTest {
    val dist = EdgeDistance.weightedAvg(reversedEdgeList)
    assert(dist == 1.1f)
  }

  "edgeDistance::simpleAvg" should "empty" in new EdgeDistanceAvgClassTest {
    val edge = EdgeDistance.simpleAvg(emptyEdgeList, false)
    assert(edge == null)

  }

  "edgeDistance::simpleAvg" should "basic" in new EdgeDistanceAvgClassTest {
    val edge = EdgeDistance.simpleAvg(basicEdgeList, false)
    assert(edge.distance == 1.2f)

  }

  "edgeDistance::simpleAvg" should "reversed" in new EdgeDistanceAvgClassTest {
    val edge = EdgeDistance.simpleAvg(reversedEdgeList, false)
    assert(edge.distance == 1.2f)
  }
}
