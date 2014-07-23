
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

import org.scalatest.{ Matchers, FlatSpec, FunSuite }
import com.intel.spark.graphon.connectedcomponents.TestingSparkContext
import com.intel.spark.graphon.communitydetection.kclique.CommunityAssigner

class CommunityAssignerTest extends FlatSpec with Matchers with TestingSparkContext {

  trait kCliqueVertexWithCommunityListTest {
    val renamedIDWithCommunity = List((1001, 1), (1002, 1), (1003, 1), (1004, 2), (1005, 2))
      .map({ case (renamedID, community) => (renamedID.toLong, community.toLong) })

    val renamedIDWithOriginalKClique = List(
      (1001, Array(2, 3, 4, 5)),
      (1002, Array(2, 3, 4, 7)),
      (1003, Array(2, 3, 4, 8)),
      (1004, Array(3, 5, 6, 7)),
      (1005, Array(3, 5, 6, 8))
    ).map({ case (renamedID, kClique) => (renamedID.toLong, kClique.map(_.toLong).toSet) })

    val vertexWithCommunityList = List(
      (2, Array(1)),
      (3, Array(1, 2)),
      (4, Array(1)),
      (5, Array(1, 2)),
      (6, Array(2)),
      (7, Array(1, 2)),
      (8, Array(1, 2))
    ).map({ case (vertex, community) => (vertex.toLong, community.map(_.toLong).toSet) })

  }

  "Assignment of communities to the vertices" should
    "produce the pair of original k-clique vertex and set of communities it belongs to" in new kCliqueVertexWithCommunityListTest {

      val rddOfVertexWithCommunityList = sc.parallelize(vertexWithCommunityList)

      val rddOfRenamedIDWithCommunity = sc.parallelize(renamedIDWithCommunity)
      val rddOfRenamedIDWithOriginalKClique = sc.parallelize(renamedIDWithOriginalKClique)

      val vertexWithAssignedCommunities = CommunityAssigner.run(rddOfRenamedIDWithCommunity, rddOfRenamedIDWithOriginalKClique)

      vertexWithAssignedCommunities.collect().toSet shouldEqual rddOfVertexWithCommunityList.collect().toSet
    }
}
