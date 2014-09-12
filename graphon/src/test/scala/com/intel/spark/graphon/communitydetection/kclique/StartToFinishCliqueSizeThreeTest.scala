
//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.spark.graphon.communitydetection.kclique

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.kclique.datatypes.{ ExtendersFact, CliqueFact, Edge }

class StartToFinishCliqueSizeThreeTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait StartToFinishClqSzThreeTest {

    //    val edgeList: List[(Long, Long)] = List((1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4), (3, 5))
    //      .map({ case (x, y) => (x.toLong, y.toLong) })
    val edgeList: List[(Long, Long)] = List((1, 3), (1, 4), (1, 5), (1, 6), (2, 3), (2, 4),
      (2, 7), (2, 8), (3, 4), (4, 7), (4, 8), (5, 6))
      .map({ case (x, y) => (x.toLong, y.toLong) })
    val rddOfEdgeList: RDD[Edge] = sparkContext.parallelize(edgeList).map(keyval => Edge(keyval._1, keyval._2))
  }

  // Test for CliqueEnumerator
  "For CliqueSize = 3, CliqueEnumerator" should
    "create all set of 3-clique extenders fact" in new StartToFinishClqSzThreeTest {

      val expectedThreeCliques = List((Array(4, 7), Array(2)), (Array(5, 6), Array(1)),
        (Array(3, 4), Array(2, 1)), (Array(4, 8), Array(2)))
        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
      val rddOfExpectedThreeCliques: RDD[ExtendersFact] = sparkContext.parallelize(expectedThreeCliques)
        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, false) })

      val cliqueEnumeratorOutput = CliqueEnumerator.run(rddOfEdgeList, 3)

      cliqueEnumeratorOutput.collect().toSet shouldEqual rddOfExpectedThreeCliques.collect().toSet
    }

  // Test for GraphGenerator vertex list
  "For CliqueSize = 3, GraphGenerator" should
    "have each 3-cliques as the vertex of the new graph" in new StartToFinishClqSzThreeTest {

      val expectedVertexListOfThreeCliqueGraph = List(Array(2, 4, 7), Array(2, 4, 8),
        Array(1, 5, 6), Array(2, 3, 4), Array(1, 3, 4))
        .map(clique => clique.map(_.toLong).toSet)
      val rddOfExpectedVertexListOfThreeCliqueGraph = sparkContext.parallelize(expectedVertexListOfThreeCliqueGraph)

      val threeCliques = List((Array(4, 7), Array(2)), (Array(5, 6), Array(1)), (Array(3, 4), Array(2, 1)), (Array(4, 8), Array(2)))
        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
      val rddOfThreeCliques = sparkContext.parallelize(threeCliques)
        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, false) })

      val threeCliqueGraphFromGraphGeneratorOutput = GraphGenerator.run(rddOfThreeCliques)
      val vertexListFromGraphGeneratorOutput = threeCliqueGraphFromGraphGeneratorOutput.cliqueGraphVertices

      vertexListFromGraphGeneratorOutput.collect().toSet shouldEqual rddOfExpectedVertexListOfThreeCliqueGraph.collect().toSet
    }

  // Test for GraphGenerator edge list
  "For CliqueSize = 3, GraphGenerator" should
    "produce correct edge list where edges between two 3-cliques " +
    "(which is the vertices of new graph) exists if they share 2 elements" in new StartToFinishClqSzThreeTest {

      val edgeListOfThreeCliqueGraph = List(
        (Array(2, 4, 7), Array(2, 3, 4)),
        (Array(2, 4, 7), Array(2, 4, 8)),
        (Array(2, 3, 4), Array(2, 4, 8)),
        (Array(2, 3, 4), Array(1, 3, 4))
      ).map(edges => (edges._1.map(_.toLong).toSet, edges._2.map(_.toLong).toSet))
      val rddOfEdgeListOfThreeCliqueGraph = sparkContext.parallelize(edgeListOfThreeCliqueGraph)

      val threeCliques = List((Array(4, 7), Array(2)), (Array(5, 6), Array(1)),
        (Array(3, 4), Array(2, 1)), (Array(4, 8), Array(2)))
        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
      val rddOfThreeCliques: RDD[ExtendersFact] = sparkContext.parallelize(threeCliques)
        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, false) })

      val threeCliqueGraphFromGraphGeneratorOutput = GraphGenerator.run(rddOfThreeCliques)
      val edgeListFromGraphGeneratorOutput = threeCliqueGraphFromGraphGeneratorOutput.cliqueGraphEdges

      edgeListFromGraphGeneratorOutput.collect().toSet shouldEqual rddOfEdgeListOfThreeCliqueGraph.collect().toSet
    }

  // Test for GetConnectedComponents
  "For CliqueSize = 3, GetConnectedComponents" should
    "produce the same number of pairs of vertices and component ID " +
    "as the number of vertices in the input graph" in new StartToFinishClqSzThreeTest {

      val threeCliques = List((Array(4, 7), Array(2)), (Array(5, 6), Array(1)),
        (Array(3, 4), Array(2, 1)), (Array(4, 8), Array(2)))
        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
      val rddOfThreeCliques: RDD[ExtendersFact] = sparkContext.parallelize(threeCliques)
        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, false) })

      val vertexListOfThreeCliqueGraph = List(Array(2, 4, 7), Array(2, 4, 8),
        Array(1, 5, 6), Array(2, 3, 4), Array(1, 3, 4))
        .map(clique => clique.map(_.toLong).toSet)
      val rddOfVertexListOfThreeCliqueGraph = sparkContext.parallelize(vertexListOfThreeCliqueGraph)

      val threeCliqueGraphFromGraphGeneratorOutput = GraphGenerator.run(rddOfThreeCliques)
      val threeCliqueGraphCCOutput = GetConnectedComponents.run(threeCliqueGraphFromGraphGeneratorOutput, sparkContext)
      val threeCliqueGraphCC = threeCliqueGraphCCOutput.connectedComponents

      threeCliqueGraphCC.count() shouldEqual vertexListOfThreeCliqueGraph.size
    }

  //  // Test for GetConnectedComponents
  //  "For CliqueSize = 3, K-Clique GetConnectedComponents" should
  //    "produce the pairs of vertices and component ID " in new StartToFinishClqSzThreeTest {
  //
  //      val threeCliques = List((Array(4, 7), Array(2)), (Array(5, 6), Array(1)),
  //        (Array(3, 4), Array(2, 1)), (Array(4, 8), Array(2)))
  //        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
  //      val rddOfThreeCliques: RDD[ExtendersFact] = sparkContext.parallelize(threeCliques)
  //        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, false) })
  //
  //      val newVertexIdToOldVertexIdOfThreeCliqueGraph = List(
  //        ("6057404231105642497".toLong, Array(2, 4, 7)),
  //        ("-3383310884846698495".toLong, Array(2, 4, 8)),
  //        ("-3656240272502685695".toLong, Array(1, 5, 6)),
  //        ("419500541110910977".toLong, Array(2, 3, 4)),
  //        ("5135849238890020865".toLong, Array(1, 3, 4))
  //      ).map(newOldPair => (newOldPair._1, newOldPair._2.map(_.toLong).toSet))
  //      val rddOfNewVertexIdToOldVertexIdOfThreeCliqueGraph = sparkContext.parallelize(newVertexIdToOldVertexIdOfThreeCliqueGraph)
  //
  //      val threeCliqueGraphFromGraphGeneratorOutput = GraphGenerator.run(rddOfThreeCliques)
  //      val threeCliqueGraphCCOutput = GetConnectedComponents.run(threeCliqueGraphFromGraphGeneratorOutput, sparkContext)
  //      val threeCliqueGraphNewToOldVertexMap = threeCliqueGraphCCOutput.newVertexIdToOldVertexIdOfCliqueGraph
  //
  //      threeCliqueGraphNewToOldVertexMap.collect().toSet shouldEqual rddOfNewVertexIdToOldVertexIdOfThreeCliqueGraph.collect().toSet
  //    }

  // Test for CommunityAssigner
  "Assignment of communities to the vertices" should
    "produce the pair of original 3-clique vertex and set of communities it belongs to" in new StartToFinishClqSzThreeTest {

      val threeCliques = List((Array(4, 7), Array(2)), (Array(5, 6), Array(1)),
        (Array(3, 4), Array(2, 1)), (Array(4, 8), Array(2)))
        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
      val rddOfThreeCliques: RDD[ExtendersFact] = sparkContext.parallelize(threeCliques)
        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, false) })

      val vertexWithNormalizedCommunity = List(
        (1, Array(1, 2)),
        (2, Array(1)),
        (3, Array(1)),
        (4, Array(1)),
        (5, Array(2)),
        (6, Array(2)),
        (7, Array(1)),
        (8, Array(1)))
        .map(vertexCommunity => (vertexCommunity._1.toLong, vertexCommunity._2.map(_.toLong).toSet))
      val rddOfVertexWithNormalizedCommunity = sparkContext.parallelize(vertexWithNormalizedCommunity)

      val threeCliqueGraphFromGraphGeneratorOutput = GraphGenerator.run(rddOfThreeCliques)
      val threeCliqueGraphCCOutput = GetConnectedComponents.run(threeCliqueGraphFromGraphGeneratorOutput, sparkContext)
      val vertexNormalizedCommunityAsCommunityAssignerOutput =
        CommunityAssigner.run(threeCliqueGraphCCOutput.connectedComponents, threeCliqueGraphCCOutput.newVertexIdToOldVertexIdOfCliqueGraph, sparkContext)

      vertexNormalizedCommunityAsCommunityAssignerOutput.collect().toSet shouldEqual rddOfVertexWithNormalizedCommunity.collect().toSet
    }
}
