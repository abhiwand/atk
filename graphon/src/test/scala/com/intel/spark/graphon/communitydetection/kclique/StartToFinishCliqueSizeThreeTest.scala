
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

import com.intel.graphbuilder.elements.{ GBEdge, Property, GBVertex }
import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.kclique.datatypes.{ CliqueExtension, Clique, Edge }

/**
 * This test checks that end-to-end run of k-clique percolation works with k = 3 on a small graph consisting of
 * two overlapping three-clique communities, a third non-overlapping three-clique community, and a leaf vertex that
 * belongs to no three-clique communities.
 *
 */
class StartToFinishCliqueSizeThreeTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait KCliquePropertyNames {
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val communityProperty = "communities"
  }

  "Three clique community analysis" should "create communities according to expected equivalance classes" in new KCliquePropertyNames {

    /*
    The graph has vertex set 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 and edge set 01, 12, 13, 23, 34, 24, 45, 46, 56, 78, 89, 79

    So that the 3-clique communities are:
    {1, 2, 3, 4}. {4 ,5 6}, {7, 8, 9} ... note that 0 belongs to no 3-clique community
    */

    val edgeSet: Set[(Long, Long)] = Set((0, 1), (1, 2), (1, 3), (2, 3), (2, 4), (3, 4), (2, 3), (4, 5), (4, 6), (5, 6), (7, 8), (7, 9), (8, 9))
      .map({ case (x, y) => (x.toLong, y.toLong) }).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val vertexSet: Set[Long] = Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val inVertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val inEdgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (outVertices, outEdges) = KCliquePercolationRunner.run(inVertexRDD, inEdgeRDD, 3, communityProperty)

    val outEdgesSet = outEdges.collect().toSet
    val outVertexSet = outVertices.collect().toSet

    outEdgesSet shouldBe gbEdgeSet

    val testVerticesToCommunities = outVertexSet.map(v => (v.gbId.value.asInstanceOf[Long],
      v.getProperty(communityProperty).get.value.asInstanceOf[java.util.Set[Long]])).toMap

    // vertex 0 gets no community (poor lonley little guy)
    testVerticesToCommunities(0) should be('empty)

    // vertices 1, 2, 3 each have only one community and it should be the same one
    testVerticesToCommunities(1).size() shouldBe 1
    testVerticesToCommunities(2) shouldBe testVerticesToCommunities(1)
    testVerticesToCommunities(3) shouldBe testVerticesToCommunities(1)

    // vertices 5 and 6 each have only one community and it should be the same one
    testVerticesToCommunities(5).size() shouldBe 1
    testVerticesToCommunities(5) shouldBe testVerticesToCommunities(6)

    // vertices 7, 8, 9 each have only one community and it should be the same one
    testVerticesToCommunities(7).size() shouldBe 1
    testVerticesToCommunities(7) shouldBe testVerticesToCommunities(8)
    testVerticesToCommunities(7) shouldBe testVerticesToCommunities(9)

    // vertex 4 belongs to the two community of {1, 2, 3} as well as the community of {5, 6}

    import collection.JavaConversions._
    testVerticesToCommunities(4).toSet shouldBe testVerticesToCommunities(1).toSet ++ testVerticesToCommunities(5).toSet
  }

}

