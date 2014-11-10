
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

import com.intel.spark.graphon.communitydetection.kclique.datatypes.Datatypes.VertexSet
import com.intel.spark.graphon.communitydetection.kclique.datatypes.{ CliqueExtension, CliqueFact }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }

/**
 * This test validates that the GraphGenerator correctly constructs a clique-shadow graph as follows:
 *
 */
class CliqueShadowGraphGeneratorTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait KCliqueGraphGenTest {

    /*
     The graph is on the vertex set 1, 2, 3, 4, 5, 6, 7, 8, 9 and the edge set is
     12, 13, 23, 24, 34, 45, 46, 56, 78, 79, 89

     so the set of triangles is
     123,  234, 456, 789

     and the 2-clique (ie. edge) shadows of the triangles are:
     12, 13, 23, 24, 34, 45, 46, 56, 78, 79, 89

     in the shadow-clique graph the edges are from the shadows to the cliques that contain them
     */

    val extensionsOfTwoCliques: Set[CliqueExtension] = Set((Set(1, 2), Set(3)), (Set(2, 3), Set(4)), (Set(4, 5), Set(6)),
      (Set(7, 8), Set(9))).map({
        case (clique, extenders) =>
          CliqueExtension(CliqueFact(clique.map(_.toLong)), extenders.map(_.toLong), neighborsHigh = true)
      })

    val triangleVertices = Set(Set(1, 2, 3), Set(2, 3, 4), Set(4, 5, 6), Set(7, 8, 9)).map(clique => clique.map(_.toLong))

    val shadowVertices = List(Array(1, 2), Array(1, 3), Array(2, 3), Array(2, 4), Array(3, 4), Array(4, 5), Array(4, 6),
      Array(5, 6), Array(7, 8), Array(7, 9), Array(8, 9)).map(clique => clique.map(_.toLong).toSet)

    val shadowGraphVertices = triangleVertices ++ shadowVertices

    val shadowGraphEdges = Set(
      (Set(1, 2, 3), Set(1, 2)), (Set(1, 2, 3), Set(1, 3)), (Set(1, 2, 3), Set(2, 3)),
      (Set(2, 3, 4), Set(2, 3)), (Set(2, 3, 4), Set(2, 4)), (Set(2, 3, 4), Set(3, 4)),
      (Set(4, 5, 6), Set(4, 5)), (Set(4, 5, 6), Set(4, 6)), (Set(4, 5, 6), Set(5, 6)),
      (Set(7, 8, 9), Set(8, 9)), (Set(7, 8, 9), Set(7, 8)), (Set(7, 8, 9), Set(7, 9))
    ).map({ case (x, y) => (x.map(_.toLong), y.map(_.toLong)) }).flatMap({ case (x, y) => Set((x, y), (y, x)) })
  }

  "K-Clique graph" should
    "have each k-cliques as the vertex of the new graph " in new KCliqueGraphGenTest {

      val twoCliqueExtensionsRDD: RDD[CliqueExtension] = sparkContext.parallelize(extensionsOfTwoCliques.toList)

      val cliqueShadowGraphOutput = CliqueShadowGraphGenerator.run(twoCliqueExtensionsRDD)

      val testVertexSet: Set[VertexSet] = cliqueShadowGraphOutput.vertices.collect().toSet
      val testEdgeSet: Set[(VertexSet, VertexSet)] = cliqueShadowGraphOutput.edges.collect().toSet

      testVertexSet shouldBe shadowGraphVertices
      testEdgeSet shouldBe shadowGraphEdges
    }
}
