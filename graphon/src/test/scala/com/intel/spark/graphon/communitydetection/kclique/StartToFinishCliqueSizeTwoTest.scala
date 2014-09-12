
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

class StartToFinishCliqueSizeTwoTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait StartToFinishClqSzTwoTest {

    //    val edgeList: List[(Long, Long)] = List((1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4), (3, 5))
    //      .map({ case (x, y) => (x.toLong, y.toLong) })
    val edgeList: List[(Long, Long)] = List((1, 3), (1, 4), (1, 5), (1, 6), (2, 3), (2, 4),
      (2, 7), (2, 8), (3, 4), (4, 7), (4, 8), (5, 6))
      .map({ case (x, y) => (x.toLong, y.toLong) })
    val rddOfEdgeList: RDD[Edge] = sparkContext.parallelize(edgeList).map(keyval => Edge(keyval._1, keyval._2))
  }

  // Test for CliqueEnumerator
  "For CliqueSize = 2, CliqueEnumerator" should
    "create all set of 2-clique extenders fact" in new StartToFinishClqSzTwoTest {

      val expectedTwoCliques = List((Array(5), Array(6)), (Array(3), Array(4)), (Array(4), Array(7, 8)),
        (Array(2), Array(3, 4, 7, 8)), (Array(1), Array(3, 4, 5, 6)))
        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
      val rddOfExpectedTwoCliques: RDD[ExtendersFact] = sparkContext.parallelize(expectedTwoCliques)
        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, true) })

      val cliqueEnumeratorOutput = CliqueEnumerator.run(rddOfEdgeList, 2)

      cliqueEnumeratorOutput.collect().toSet shouldEqual rddOfExpectedTwoCliques.collect().toSet
    }

  // Test for GraphGenerator vertex list
  "For CliqueSize = 2, GraphGenerator" should
    "have each 2-cliques as the vertex of the new graph" in new StartToFinishClqSzTwoTest {

      val expectedVertexListOfTwoCliqueGraph = List(Array(1, 3), Array(1, 4), Array(1, 5), Array(1, 6),
        Array(2, 3), Array(2, 4), Array(2, 7), Array(2, 8),
        Array(3, 4), Array(4, 7), Array(4, 8), Array(5, 6))
        .map(clique => clique.map(_.toLong).toSet)
      val rddOfExpectedVertexListOfTwoCliqueGraph = sparkContext.parallelize(expectedVertexListOfTwoCliqueGraph)

      val twoCliques = List((Array(5), Array(6)), (Array(3), Array(4)), (Array(4), Array(7, 8)),
        (Array(2), Array(3, 4, 7, 8)), (Array(1), Array(3, 4, 5, 6)))
        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
      val rddOfTwoCliques = sparkContext.parallelize(twoCliques)
        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, true) })

      val twoCliqueGraphFromGraphGeneratorOutput = GraphGenerator.run(rddOfTwoCliques)
      val vertexListFromGraphGeneratorOutput = twoCliqueGraphFromGraphGeneratorOutput.cliqueGraphVertices

      vertexListFromGraphGeneratorOutput.collect().toSet shouldEqual
        rddOfExpectedVertexListOfTwoCliqueGraph.collect().toSet
    }

  // Test for GraphGenerator edge list
  "For CliqueSize = 2, GraphGenerator" should
    "produce correct edge list where edges between two 2-cliques " +
    "(which is the vertices of new graph) exists if they share 1 element" in new StartToFinishClqSzTwoTest {

      val edgeListOfTwoCliqueGraph = List(
        (Array(2, 4), Array(4, 7)), (Array(3, 4), Array(4, 8)), (Array(1, 4), Array(2, 4)),
        (Array(2, 3), Array(2, 8)), (Array(3, 4), Array(1, 4)), (Array(1, 4), Array(4, 7)),
        (Array(1, 4), Array(1, 6)), (Array(1, 3), Array(1, 6)), (Array(3, 4), Array(2, 4)),
        (Array(4, 7), Array(2, 7)), (Array(2, 7), Array(2, 8)), (Array(1, 3), Array(1, 5)),
        (Array(1, 4), Array(1, 5)), (Array(5, 6), Array(1, 6)),
        (Array(2, 3), Array(1, 3)), (Array(2, 3), Array(2, 4)), (Array(2, 4), Array(2, 7)),
        (Array(1, 3), Array(1, 4)), (Array(5, 6), Array(1, 5)), (Array(1, 4), Array(4, 8)),
        (Array(3, 4), Array(2, 3)), (Array(2, 3), Array(2, 7)), (Array(1, 5), Array(1, 6)),
        (Array(3, 4), Array(1, 3)), (Array(4, 7), Array(4, 8)), (Array(2, 4), Array(4, 8)),
        (Array(4, 8), Array(2, 8)), (Array(3, 4), Array(4, 7)), (Array(2, 4), Array(2, 8))
      ).map(edges => (edges._1.map(_.toLong).toSet, edges._2.map(_.toLong).toSet))
      val rddOfEdgeListOfTwoCliqueGraph = sparkContext.parallelize(edgeListOfTwoCliqueGraph)

      val twoCliques = List((Array(5), Array(6)), (Array(3), Array(4)), (Array(4), Array(7, 8)),
        (Array(2), Array(3, 4, 7, 8)), (Array(1), Array(3, 4, 5, 6)))
        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
      val rddOfTwoCliques: RDD[ExtendersFact] = sparkContext.parallelize(twoCliques)
        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, true) })

      val twoCliqueGraphFromGraphGeneratorOutput = GraphGenerator.run(rddOfTwoCliques)
      val edgeListFromGraphGeneratorOutput = twoCliqueGraphFromGraphGeneratorOutput.cliqueGraphEdges

      edgeListFromGraphGeneratorOutput.collect().toSet shouldEqual
        rddOfEdgeListOfTwoCliqueGraph.collect().toSet
    }

  // Test for GetConnectedComponents
  "For CliqueSize = 2, K-Clique GetConnectedComponents" should
    "produce the same number of pairs of vertices and component ID " +
    "as the number of vertices in the input graph" in new StartToFinishClqSzTwoTest {

      val twoCliques = List((Array(5), Array(6)), (Array(3), Array(4)), (Array(4), Array(7, 8)),
        (Array(2), Array(3, 4, 7, 8)), (Array(1), Array(3, 4, 5, 6)))
        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
      val rddOfTwoCliques: RDD[ExtendersFact] = sparkContext.parallelize(twoCliques)
        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, true) })

      val vertexListOfTwoCliqueGraph = List(Array(1, 3), Array(1, 4), Array(1, 5), Array(1, 6),
        Array(2, 3), Array(2, 4), Array(2, 7), Array(2, 8),
        Array(3, 4), Array(4, 7), Array(4, 8), Array(5, 6))
        .map(clique => clique.map(_.toLong).toSet)
      val rddOfVertexListOfTwoCliqueGraph = sparkContext.parallelize(vertexListOfTwoCliqueGraph)

      val twoCliqueGraphFromGraphGeneratorOutput = GraphGenerator.run(rddOfTwoCliques)
      val twoCliqueGraphCCOutput = GetConnectedComponents.run(twoCliqueGraphFromGraphGeneratorOutput, sparkContext)
      val twoCliqueGraphCC = twoCliqueGraphCCOutput.connectedComponents

      twoCliqueGraphCC.count() shouldEqual vertexListOfTwoCliqueGraph.size
    }

  // Test for CommunityAssigner
  "Assignment of communities to the vertices" should
    "produce the pair of original 2-clique vertex and set of communities it belongs to" in new StartToFinishClqSzTwoTest {

      val twoCliques = List((Array(5), Array(6)), (Array(3), Array(4)), (Array(4), Array(7, 8)),
        (Array(2), Array(3, 4, 7, 8)), (Array(1), Array(3, 4, 5, 6)))
        .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })
      val rddOfTwoCliques: RDD[ExtendersFact] = sparkContext.parallelize(twoCliques)
        .map({ case (x, y) => ExtendersFact(CliqueFact(x), y, true) })

      val vertexWithNormalizedCommunity = List(
        (1, Array(1)),
        (2, Array(1)),
        (3, Array(1)),
        (4, Array(1)),
        (5, Array(1)),
        (6, Array(1)),
        (7, Array(1)),
        (8, Array(1)))
        .map(vertexCommunity => (vertexCommunity._1.toLong, vertexCommunity._2.map(_.toLong).toSet))
      val rddOfVertexWithNormalizedCommunity = sparkContext.parallelize(vertexWithNormalizedCommunity)

      val twoCliqueGraphFromGraphGeneratorOutput = GraphGenerator.run(rddOfTwoCliques)
      val twoCliqueGraphCCOutput = GetConnectedComponents.run(twoCliqueGraphFromGraphGeneratorOutput, sparkContext)
      val vertexNormalizedCommunityAsCommunityAssignerOutput =
        CommunityAssigner.run(twoCliqueGraphCCOutput.connectedComponents, twoCliqueGraphCCOutput.newVertexIdToOldVertexIdOfCliqueGraph, sparkContext)

      vertexNormalizedCommunityAsCommunityAssignerOutput.collect().toSet shouldEqual rddOfVertexWithNormalizedCommunity.collect().toSet
    }
}
