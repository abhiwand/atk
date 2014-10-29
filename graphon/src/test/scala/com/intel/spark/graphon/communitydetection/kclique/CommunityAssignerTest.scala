
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
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.spark.graphon.communitydetection.kclique.datatypes.Datatypes.VertexSet

class CommunityAssignerTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait kCliqueVertexWithCommunityListTest {

    /*
     The graph has vertex set 1, 2, 3, 4, 5, 6, 7
     and edge set 12, 13, 14, 23,24, 34, 35, 36, 37, 46, 46, 47, 56, 57

     so there are three 4-cliques:  1234, 3456, and 3457

     1234 is in its own 4-clique community
     3456 and 3457 are in another 4-clique community
     */

    val clique1: VertexSet = Set(1, 2, 3, 4).map(_.toLong)
    val clique2: VertexSet = Set(3, 4, 5, 6).map(_.toLong)
    val clique3: VertexSet = Set(3, 4, 5, 7).map(_.toLong)

    val cliquesToCommunities = Seq(Pair(clique1, 1.toLong), Pair(clique2, 2.toLong), Pair(clique3, 2.toLong))

  }

  "Assignment of communities to the vertices" should
    "produce the pair of original k-clique vertex and set of communities it belongs to" in new kCliqueVertexWithCommunityListTest {

      val cliquesToCommunitiesRDD = sparkContext.parallelize(cliquesToCommunities)

      val verticesToCommunities = CommunityAssigner.run(cliquesToCommunitiesRDD)

      val vertexToCommunitiesMap = verticesToCommunities.collect().toMap

      vertexToCommunitiesMap(1.toLong) should be(Set(1.toLong))
      vertexToCommunitiesMap(2.toLong) should be(Set(1.toLong))

      vertexToCommunitiesMap(3.toLong) should be(Set(1.toLong, 2.toLong))
      vertexToCommunitiesMap(4.toLong) should be(Set(1.toLong, 2.toLong))

      vertexToCommunitiesMap(5.toLong) should be(Set(2.toLong))
      vertexToCommunitiesMap(6.toLong) should be(Set(2.toLong))
      vertexToCommunitiesMap(7.toLong) should be(Set(2.toLong))

    }
}

