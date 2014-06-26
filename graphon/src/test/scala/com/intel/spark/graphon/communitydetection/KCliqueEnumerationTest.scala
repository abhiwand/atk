package com.intel.spark.graphon.communitydetection

import org.scalatest.{ Matchers, FlatSpec, FunSuite }
import com.intel.spark.graphon.connectedcomponents.TestingSparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._

class KCliqueEnumerationTest extends FlatSpec with Matchers with TestingSparkContext {

  trait KCliqueEnumTest {
    val vertexWithAdjacencyList: List[(Long, Array[Long])] = List((1, Array(2, 3, 4)), (2, Array(3, 4)), (3, Array(4, 5))).map(
      { case (v, nbrs) => (v.toLong, nbrs.map(_.toLong)) })
    val vertexWithAdjacencyRDD = sc.parallelize(vertexWithAdjacencyList).map(keyval => VertexInAdjacencyFormat(keyval._1, keyval._2))

    val edgeList: List[(Long, Long)] = List((1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4), (3, 5)).map({ case (x, y) => (x.toLong, y.toLong) })
    val edgeListRDD: RDD[Edge] = sc.parallelize(edgeList).map(keyval => Edge(keyval._1, keyval._2))

    val fourCliques = List((Array(1, 2, 3), Array(4))).map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
    val fourCliquesRDD = sc.parallelize(fourCliques).map({ case (x, y) => (CliqueFact(x), y, true) })

  }
  /*
  "creating edge list from adjacency list" should "create correct edge list" in new KCliqueEnumTest {

    val kcliqueEdgeList = KCliqueEnumeration.createEdgeListFromParsedAdjList(vertexWithAdjacencyRDD)

    kcliqueEdgeList shouldEqual edgeListRDD

  }
*/
  /*
  "K-Clique enumeration" should "create all set of k-cliques" in new KCliqueEnumTest {

    val enumeratedFourCliques = KCliqueEnumeration.applyToAdjacencyList(vertexWithAdjacencyRDD, 4)

    enumeratedFourCliques shouldEqual fourCliquesRDD

  }
  */

}