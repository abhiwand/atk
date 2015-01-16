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

package com.intel.spark.graphon.graphstatistics

import com.intel.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import com.intel.spark.graphon.GraphStatistics
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }

/**
 * Test the behavior of the degree calculation routines on a property graph with two distinct edge labels.
 *
 * The graph is on the integers 1 through 20 with two edges:  divisorOf and multipleOf
 * with a divisorOf edge from a to b when a | b and a multipleOf edge from a to b when b|a
 *
 */
class FactorizationPropertyGraphTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val defaultParallelism = 3 // use of parellelism > 1 to catch stupid parallelization bugs

  trait FactorizationPGraphTest {

    val divisorOfLabel = "divisorOf"
    val multipleOfLabel = "multipleOf"

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = (1 to 20).toList.map(_.toLong)

    val numToDivisors: Map[Long, Set[Int]] = Map(
      1.toLong -> Set(1),
      2.toLong -> Set(1, 2),
      3.toLong -> Set(1, 3),
      4.toLong -> Set(1, 2, 4),
      5.toLong -> Set(1, 5),
      6.toLong -> Set(1, 2, 3, 6),
      7.toLong -> Set(1, 7),
      8.toLong -> Set(1, 2, 4, 8),
      9.toLong -> Set(1, 3, 9),
      10.toLong -> Set(1, 2, 5, 10),
      11.toLong -> Set(1, 11),
      12.toLong -> Set(1, 2, 3, 4, 6, 12),
      13.toLong -> Set(1, 13),
      14.toLong -> Set(1, 2, 7, 14),
      15.toLong -> Set(1, 3, 5, 15),
      16.toLong -> Set(1, 2, 4, 8, 16),
      17.toLong -> Set(1, 17),
      18.toLong -> Set(1, 2, 3, 6, 9, 18),
      19.toLong -> Set(1, 19),
      20.toLong -> Set(1, 2, 4, 5, 10, 20)
    )

    val numToMultiples: Map[Long, Set[Int]] = Map(
      1.toLong -> (1 to 20).toSet,
      2.toLong -> Set(2, 4, 6, 8, 10, 12, 14, 16, 18, 20),
      3.toLong -> Set(3, 6, 9, 12, 15, 18),
      4.toLong -> Set(4, 8, 12, 16, 20),
      5.toLong -> Set(5, 10, 15, 20),
      6.toLong -> Set(6, 12, 18),
      7.toLong -> Set(7, 14),
      8.toLong -> Set(8, 16),
      9.toLong -> Set(9, 18),
      10.toLong -> Set(10, 20),
      11.toLong -> Set(11),
      12.toLong -> Set(12),
      13.toLong -> Set(13),
      14.toLong -> Set(14),
      15.toLong -> Set(15),
      16.toLong -> Set(16),
      17.toLong -> Set(17),
      18.toLong -> Set(18),
      19.toLong -> Set(19),
      20.toLong -> Set(20)
    )

    val divisorEdgeList: List[(Long, Long)] =
      numToDivisors.toList.flatMap({ case (i, divisorSet) => divisorSet.map(d => (d.toLong, i.toLong)) })

    val multiplesEdgeList: List[(Long, Long)] =
      numToMultiples.toList.flatMap({ case (i, multiples) => multiples.map(m => (m.toLong, i.toLong)) })

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbDivisorEdgeList =
      divisorEdgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            divisorOfLabel, Set.empty[Property])
      })

    val gbMultipleEdgeList =
      multiplesEdgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            multipleOfLabel, Set.empty[Property])
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbDivisorEdgeList.union(gbMultipleEdgeList))

    val expectedDivisorInDegreeOutput: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, (numToDivisors(v.physicalId.asInstanceOf[Long]).size.toLong))).toSet

    val expectedMultipleInDegreeOutput: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, numToMultiples(v.physicalId.asInstanceOf[Long]).size.toLong)).toSet

    val expectedDivisorOutDegreeOutput = expectedMultipleInDegreeOutput
    val expectedMultipleOutDegreeOutput = expectedDivisorInDegreeOutput
  }

  "factorization graph" should "have correct divisor in-degree" in new FactorizationPGraphTest {
    val results = GraphStatistics.inDegreesByEdgeLabel(vertexRDD, edgeRDD, divisorOfLabel)
    results.collect().toSet shouldEqual expectedDivisorInDegreeOutput
  }

  "factorization graph" should "have correct divisor out-degree" in new FactorizationPGraphTest {
    val results = GraphStatistics.outDegreesByEdgeLabel(vertexRDD, edgeRDD, divisorOfLabel)
    results.collect().toSet shouldEqual expectedDivisorOutDegreeOutput
  }

  "factorization graph" should "have correct multiple-of in-degree" in new FactorizationPGraphTest {

    val results = GraphStatistics.inDegreesByEdgeLabel(vertexRDD, edgeRDD, multipleOfLabel)
    results.collect().toSet shouldEqual expectedMultipleInDegreeOutput
  }

  "factorization graph" should "have correct multiple-of out-degree" in new FactorizationPGraphTest {
    val results = GraphStatistics.outDegreesByEdgeLabel(vertexRDD, edgeRDD, multipleOfLabel)
    results.collect().toSet shouldEqual expectedMultipleOutDegreeOutput
  }
}
