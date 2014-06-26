package com.intel.spark.graphon.communitydetection

import org.scalatest.{ Matchers, FlatSpec, FunSuite }
import com.intel.spark.graphon.connectedcomponents.TestingSparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._

class CreateGraphFromEnumeratedKCliquesTest extends FlatSpec with Matchers with TestingSparkContext {

  trait KCliqueGraphTest {

    val fourCliques = List((Array(2, 3, 4), Array(5, 7, 8)), (Array(3, 5, 6), Array(7, 8))).map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
    val fourCliquesRDD = sc.parallelize(fourCliques).map({ case (x, y) => (CliqueFact(x), y, true) })

    val fourCliqueGraph = List((Array(2, 3, 4), Array(Array(2, 3, 4, 5), Array(2, 3, 4, 7), Array(2, 3, 4, 8))), (Array(3, 5, 6), Array(Array(3, 5, 6, 7), Array(3, 5, 6, 8))))
      .map({ case (clique, cliqueSet) => (clique.map(_.toLong).toSet, cliqueSet.map(_.map(_.toLong).toSet).toSet) })
    val fourCliqueGraphRDD = sc.parallelize(fourCliqueGraph).map({ case (clique, cliqueSet) => (CliqueFact(clique), cliqueSet.map(x => CliqueFact(x))) })

    val fourCliqueEdgeList = List(Array(Array(2, 3, 4, 5), Array(2, 3, 4, 7)), Array(Array(2, 3, 4, 5), Array(2, 3, 4, 8)), Array(Array(2, 3, 4, 7), Array(2, 3, 4, 8)), Array(Array(3, 5, 6, 7), Array(3, 5, 6, 8))).map(_.map(_.map(_.toLong).toSet).toSet)
    val fourCliqueEdgeListRDD = sc.parallelize(fourCliqueEdgeList).map(_.map(x => CliqueFact(x)))
  }

  /*
  "K-Clique graph" should "create correct pairs of Clique and K-cliques extending it" in new KCliqueGraphTest {

    val cliqueAndExtendedCliqueSet = CreateGraphFromEnumeratedKCliques.createCliqueAndExtendedCliqueSet(fourCliquesRDD)

    cliqueAndExtendedCliqueSet shouldEqual fourCliqueGraphRDD

  }
  */

  /*
  "K-Clique graph" should "create correct pairs of Clique edges" in new KCliqueGraphTest {

    val cliqueEdgeList = CreateGraphFromEnumeratedKCliques.run(fourCliquesRDD)

    cliqueEdgeList shouldEqual fourCliqueEdgeListRDD

  }
  */
}