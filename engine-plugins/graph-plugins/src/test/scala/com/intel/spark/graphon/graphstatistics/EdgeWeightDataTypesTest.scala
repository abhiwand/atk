//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }

/**
 * This test checks that edge weight calculations can be performed correctly when the edge properties contain
 * numerical data and that the appropriate exception is thrown when they do not.
 */
class EdgeWeightDataTypesTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val defaultParallelism = 3

  // use of value > 1 to catch stupid parallelization bugs

  trait LongWeightsTest {

    val weightProperty = "waitwaitdon'ttellme"
    val weightPropertyOption = Some(weightProperty)
    val weight = 3L
    val missingProperty = "the missing"
    val missingPropertyOption = Some(missingProperty)
    val defaultWeight = 1L

    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((1L, 2L))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set(Property(weightProperty, weight)))
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

    private val invalidWeightedDegreees: Map[Long, Double] = Map(1L -> 0D, 2L -> 0D)
    private val validWeightedInDegrees: Map[Long, Double] = Map(1L -> 0D, 2L -> weight)
    private val validWeightedOutDegrees: Map[Long, Double] = Map(1L -> weight, 2L -> 0D)
    private val defaultWeightedInDegrees: Map[Long, Double] = Map(1L -> 0D, 2L -> defaultWeight)
    private val defaultWeightedOutDegrees: Map[Long, Double] = Map(1L -> defaultWeight, 2L -> 0D)

    val expectedOutputInDegreeValidLabel =
      gbVertexList.map(v => (v, validWeightedInDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputOutDegreeValidLabel =
      gbVertexList.map(v => (v, validWeightedOutDegrees(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedOutputInDegreeDefault =
      gbVertexList.map(v => (v, defaultWeightedInDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputOutDegreeDefault =
      gbVertexList.map(v => (v, defaultWeightedOutDegrees(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedOutputInvalidLabel =
      gbVertexList.map(v => (v, invalidWeightedDegreees(v.physicalId.asInstanceOf[Long]))).toSet
  }

  "single directed edge with long weight" should "have correct in-weight" in new LongWeightsTest {

    val results = WeightedDegrees.inWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight)

    results.collect().toSet shouldEqual expectedOutputInDegreeValidLabel
  }

  trait IntWeightsTest {

    val weightProperty = "waitwaitdon'ttellme"
    val weightPropertyOption = Some(weightProperty)
    val weight = 3
    val missingProperty = "the missing"
    val missingPropertyOption = Some(missingProperty)
    val defaultWeight = 1

    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((1L, 2L))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set(Property(weightProperty, weight)))
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

    private val invalidWeightedDegreees: Map[Long, Double] = Map(1L -> 0D, 2L -> 0D)
    private val validWeightedInDegrees: Map[Long, Double] = Map(1L -> 0D, 2L -> weight)
    private val validWeightedOutDegrees: Map[Long, Double] = Map(1L -> weight, 2L -> 0D)
    private val defaultWeightedInDegrees: Map[Long, Double] = Map(1L -> 0D, 2L -> defaultWeight)
    private val defaultWeightedOutDegrees: Map[Long, Double] = Map(1L -> defaultWeight, 2L -> 0D)

    val expectedOutputInDegreeValidLabel =
      gbVertexList.map(v => (v, validWeightedInDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputOutDegreeValidLabel =
      gbVertexList.map(v => (v, validWeightedOutDegrees(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedOutputInDegreeDefault =
      gbVertexList.map(v => (v, defaultWeightedInDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputOutDegreeDefault =
      gbVertexList.map(v => (v, defaultWeightedOutDegrees(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedOutputInvalidLabel =
      gbVertexList.map(v => (v, invalidWeightedDegreees(v.physicalId.asInstanceOf[Long]))).toSet
  }

  "single directed edge with Int weight" should "have correct in-weight" in new IntWeightsTest {

    val results = WeightedDegrees.inWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight)

    results.collect().toSet shouldEqual expectedOutputInDegreeValidLabel
  }

  trait FloatWeightsTest {

    val weightProperty = "waitwaitdon'ttellme"
    val weightPropertyOption = Some(weightProperty)
    val weight = 3.0f
    val missingProperty = "the missing"
    val missingPropertyOption = Some(missingProperty)
    val defaultWeight = 1.0f

    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((1L, 2L))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set(Property(weightProperty, weight)))
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

    private val invalidWeightedDegreees: Map[Long, Double] = Map(1L -> 0D, 2L -> 0D)
    private val validWeightedInDegrees: Map[Long, Double] = Map(1L -> 0D, 2L -> weight)
    private val validWeightedOutDegrees: Map[Long, Double] = Map(1L -> weight, 2L -> 0D)
    private val defaultWeightedInDegrees: Map[Long, Double] = Map(1L -> 0D, 2L -> defaultWeight)
    private val defaultWeightedOutDegrees: Map[Long, Double] = Map(1L -> defaultWeight, 2L -> 0D)

    val expectedOutputInDegreeValidLabel =
      gbVertexList.map(v => (v, validWeightedInDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputOutDegreeValidLabel =
      gbVertexList.map(v => (v, validWeightedOutDegrees(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedOutputInDegreeDefault =
      gbVertexList.map(v => (v, defaultWeightedInDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputOutDegreeDefault =
      gbVertexList.map(v => (v, defaultWeightedOutDegrees(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedOutputInvalidLabel =
      gbVertexList.map(v => (v, invalidWeightedDegreees(v.physicalId.asInstanceOf[Long]))).toSet
  }

  "single directed edge with Float weight" should "have correct in-weight" in new FloatWeightsTest {

    val results = WeightedDegrees.inWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight)

    results.collect().toSet shouldEqual expectedOutputInDegreeValidLabel
  }

  trait StringWeightsTest {

    val weightProperty = "waitwaitdon'ttellme"
    val weightPropertyOption = Some(weightProperty)
    val weight: String = "this is not my beautiful edge weight!"
    val missingProperty = "the missing"
    val missingPropertyOption = Some(missingProperty)
    val defaultWeight = 1.0f

    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((1L, 2L))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set(Property(weightProperty, weight)))
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

  }

  "single directed edge with String weight" should "throw a Sparkexception" in new StringWeightsTest {
    intercept[org.apache.spark.SparkException] {
      val results = WeightedDegrees.inWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight).collect()
    }
  }
}
