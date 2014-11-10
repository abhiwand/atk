
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
import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.{ FlatSpec, Matchers }

class GetConnectedComponentsTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait getConnectedComponentsTest {
    /*
     The test graph is on the vertex set 1, 2, 3, 4, 5, 6,  and the edge set is
     1-2, 2-3, 3-4, 1-3, 2-4, 4-5, 4-6, 5-6,
     so it has the cliques 123, 234, and 456

    */

    val clique1: VertexSet = Set(1.toLong, 2.toLong, 3.toLong)
    val clique2: VertexSet = Set(2.toLong, 3.toLong, 4.toLong)
    val clique3: VertexSet = Set(4.toLong, 5.toLong, 6.toLong)

    val cliqueGraphVertexSet = Seq(clique1, clique2, clique3)
    val cliqueGraphEdgeSet = Seq(Pair(clique1, clique2))

  }

  "K-Clique Connected Components" should
    "produce the same number of pairs of vertices and component ID as the number of vertices in the input graph" in new getConnectedComponentsTest {

      val vertexRDD = sparkContext.parallelize(cliqueGraphVertexSet)
      val edgeRDD = sparkContext.parallelize(cliqueGraphEdgeSet)

      val cliquesToCommunities = GetConnectedComponents.run(vertexRDD, edgeRDD)

      val cliquesToCommunitiesMap = cliquesToCommunities.collect().toMap

      cliquesToCommunitiesMap.keySet should contain(clique1)
      cliquesToCommunitiesMap.keySet should contain(clique2)
      cliquesToCommunitiesMap.keySet should contain(clique3)
    }
}
