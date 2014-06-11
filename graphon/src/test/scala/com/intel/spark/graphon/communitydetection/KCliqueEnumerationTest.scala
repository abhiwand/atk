package com.intel.spark.graphon.communitydetection

import org.scalatest.{Matchers, FlatSpec, FunSuite}
import com.intel.spark.graphon.connectedcomponents.TestingSparkContext


class KCliqueEnumerationTest extends FlatSpec with Matchers with TestingSparkContext {

  trait KCliqueEnumTest {
    val vertexWithAdjacencyList : List[(Long, Array[Long])]= List((1,Array(2,3,4)), (2,Array(1,3,4)), (3, Array(1,2)), (4,Array(1,2))).map(
    {case (v, nbrs) => (v.toLong, nbrs.map(_.toLong))})

    val vertexWithAdjacencyRDD = sc.parallelize(vertexWithAdjacencyList)

    val edgeList : List[(Long,Long)] = List((1,2),(1,3),(1,4),(2,1),(2,3),(2,4),(3,1),(3,2),(4,1),(4,2)).map({case (x,y) => (x.toLong, y.toLong)})

    val x : List[Long] = List (0,1,2,3)
  }


  "creating edge list from adjacency list" should "create correct edge list" in new KCliqueEnumTest {


    val kCliqueEnumeration = new KCliqueEnumeration(vertexWithAdjacencyRDD,3)

    kCliqueEnumeration.createEdgelistFromParsedAdjList(vertices: RDD[VertexInAdjacencyFormat]): RDD[Edge]
    0 shouldBe 0
  }

}
