package com.intel.spark.graphon.communitydetection.kclique

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.spark.graphon.connectedcomponents.TestingSparkContext
import org.apache.spark.rdd.RDD
import DataTypes._

class CliqueEnumeratorTest extends FlatSpec with Matchers with TestingSparkContext {

  trait KCliqueEnumTest {

    val vertexWithAdjacencyList: List[(Long, Array[Long])] = List((1, Array(2, 3, 4)), (2, Array(3, 4)), (3, Array(4, 5))).map(
      { case (v, nbrs) => (v.toLong, nbrs.map(_.toLong)) })

    val edgeList: List[(Long, Long)] = List((1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4), (3, 5)).map({ case (x, y) => (x.toLong, y.toLong) })

    val fourCliques = List((Array(1, 2, 3), Array(4))).map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })

  }

  "Creating edge list from adjacency list" should
    "create correct edge list" in new KCliqueEnumTest {

      val rddOfVertexWithAdjacencyList: RDD[VertexInAdjacencyFormat] = sc.parallelize(vertexWithAdjacencyList).map(keyval => VertexInAdjacencyFormat(keyval._1, keyval._2))
      val rddOfEdgeList: RDD[Edge] = sc.parallelize(edgeList).map(keyval => Edge(keyval._1, keyval._2))

      val kcliqueEdgeList = CliqueEnumerator.createEdgeListFromParsedAdjList(rddOfVertexWithAdjacencyList)

      kcliqueEdgeList.collect().toSet shouldEqual rddOfEdgeList.collect().toSet

    }

  "K-Clique enumeration" should
    "create all set of k-cliques" in new KCliqueEnumTest {

      val rddOfVertexWithAdjacencyList: RDD[VertexInAdjacencyFormat] = sc.parallelize(vertexWithAdjacencyList).map(keyval => VertexInAdjacencyFormat(keyval._1, keyval._2))
      val rddOfFourCliques = sc.parallelize(fourCliques).map({ case (x, y) => ExtendersFact(CliqueFact(x), y, true) })

      val enumeratedFourCliques = CliqueEnumerator.applyToAdjacencyList(rddOfVertexWithAdjacencyList, 4)

      enumeratedFourCliques.collect().toSet shouldEqual rddOfFourCliques.collect().toSet

    }

}