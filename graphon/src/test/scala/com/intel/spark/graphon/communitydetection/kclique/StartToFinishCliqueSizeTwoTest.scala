
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

import com.intel.graphbuilder.elements.{Edge, Property, Vertex}
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.kclique.datatypes.{ CliqueExtension, CliqueFact, Edge }

/**
 * This test checks that end-to-end run of k-clique percolation works with k = 2 on a small graph consisting of
 * two non-trivial connected components and an isolated vertex.
 *
 * It is intended as "base case" functionality.
 */

class StartToFinishCliqueSizeTwoTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {


  trait KCliquePropertyNames {
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val communityProperty = "communities"
  }

  
  
  "Two clique community analysis" should "create communities according to expected equivalance classes" in new KCliquePropertyNames  {

   /*
   The graph has vertex set 1, 2, 3 , 4, 5, 6, 7 and edge set 13, 35, 24, 26, 46
   So that the 2-clique communities (the connected components that are not isolated vertices) are going to be:
   {1, 3, 5},  {2, 4, 6},  Note that 7 belongs to no 2 clique community.
   */

    val edgeSet: Set[(Long, Long)] = Set((1,3), (3,5), (2,4), (2,6), (4,6))
      .map({ case (x, y) => (x.toLong, y.toLong) })

    val vertexSet: Set[Long] = Set(1,2,3,4,5,6,7)


    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })
    

    val  inVertexRDD : RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val  inEdgeRDD : RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (outVertices, outEdges) = KCliquePercolationRunner.run(inVertexRDD, inEdgeRDD, 2, communityProperty)

    val outEdgesSet = outEdges.collect().toSet
    val outVertexSet = outVertices.collect().toSet

    outEdgesSet shouldBe gbEdgeSet

    val testVerticesToCommunities = outVertexSet.map(v => (v.gbId.value.asInstanceOf[Long],
      v.getProperty(communityProperty).get.value.asInstanceOf[java.util.Set[Long]])).toMap

    // vertex 7 gets no community (poor lonley little guy)
    testVerticesToCommunities(7) should be ('empty)

    // no vertex gets more than one two-clique community (this is a general property of two-clique communities...
    // they're just the connected components of the non-isolated vertices)

    testVerticesToCommunities(1).size() shouldBe 1
    testVerticesToCommunities(2).size() shouldBe 1

    // vertex 1 and vertex 2 get distinct two-clique communities
    testVerticesToCommunities(1) should not be testVerticesToCommunities(2)

    // vertex 3 and vertex 5 have the same two-clique community as vertex 1
    testVerticesToCommunities(3) shouldBe testVerticesToCommunities(1)
    testVerticesToCommunities(5) shouldBe testVerticesToCommunities(1)

    // vertex 4 and vertex 6 have the same two-clique community as vertex 2
    testVerticesToCommunities(4) shouldBe testVerticesToCommunities(2)
    testVerticesToCommunities(6) shouldBe testVerticesToCommunities(2)
  }


}
