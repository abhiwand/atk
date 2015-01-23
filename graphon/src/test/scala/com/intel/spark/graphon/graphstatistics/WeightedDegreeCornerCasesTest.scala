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
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }

/**
 * Exercises the degree calculation utilities on trivial and malformed graphs.
 */
class WeightedDegreeCornerCasesTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val defaultParallelism = 3 // use of value > 1 to catch stupid parallelization bugs

  "empty graph" should "result in empty results" in {

    val weightProperty = "waitwaitdon'ttellme"
    val weightPropertyOption = Some(weightProperty)
    val defaultWeight = 0D
    val vertexRDD = sparkContext.parallelize(List.empty[GBVertex], defaultParallelism)
    val edgeRDD = sparkContext.parallelize(List.empty[GBEdge], defaultParallelism)

    WeightedDegrees.outWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight).count() shouldBe 0D
    WeightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set("edge label"))).count() shouldBe 0
    WeightedDegrees.inWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight).count() shouldBe 0
    WeightedDegrees.inWeightByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set("edge label"))).count() shouldBe 0
  }

  "single node graph" should "have all edge labels net weight  0" in {

    val weightProperty = "waitwaitdon'ttellme"
    val weightPropertyOption = Some(weightProperty)
    val defaultWeight = 0D

    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1)
    val edgeList: List[(Long, Long)] = List()

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set.empty[Property])
      })

    val vertexRDD = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD = sparkContext.parallelize(gbEdgeList, defaultParallelism)

    val expectedOutput =
      gbVertexList.map(v => (v, 0L)).toSet

    WeightedDegrees.outWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight).collect().toSet shouldBe expectedOutput
    WeightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(validEdgeLabel))).collect().toSet shouldBe expectedOutput
    WeightedDegrees.inWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight).collect().toSet shouldBe expectedOutput
    WeightedDegrees.inWeightByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(validEdgeLabel))).collect().toSet shouldBe expectedOutput
    WeightedDegrees.undirectedWeightedDegree(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight).collect().toSet shouldBe expectedOutput
    WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(validEdgeLabel))).collect().toSet shouldBe expectedOutput
  }

  trait SingleUndirectedEdgeTest {

    val weightProperty = "Wait wait! Don't tell me!"
    val weightPropertyOption = Some(weightProperty)
    val weight = 0.5D
    val missingProperty = "the missing"
    val missingPropertyOption = Some(missingProperty)
    val defaultWeight = 0.3D

    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((1L, 2L), (2L, 1L))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set(Property(weightProperty, weight)))
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

    private val allZeroWeights: Map[Long, Double] = Map(1L -> 0D, 2L -> 0D)
    private val netWeights: Map[Long, Double] = Map(1L -> weight, 2L -> weight)
    private val defaultWeights: Map[Long, Double] = Map(1L -> defaultWeight, 2L -> defaultWeight)

    val expectedOutputValidLabel = gbVertexList.map(v => (v, netWeights(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputValidLabelDefaultWeight =
      gbVertexList.map(v => (v, defaultWeights(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputAllZeroDegrees = gbVertexList.map(v => (v, allZeroWeights(v.physicalId.asInstanceOf[Long]))).toSet
  }

  "single undirected edge" should "have correct in-weight" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.inWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight)
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct in-weight for valid label" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.inWeightByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have zero in-weight for invalid label" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.inWeightByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(invalidEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  "single undirected edge" should "have default in-weight when label is valid, property is missing" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.inWeightByEdgeLabel(vertexRDD, edgeRDD, missingPropertyOption, defaultWeight, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputValidLabelDefaultWeight
  }

  "single undirected edge" should "have default in-weight when property is missing" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.inWeight(vertexRDD, edgeRDD, missingPropertyOption, defaultWeight)
    results.collect().toSet shouldEqual expectedOutputValidLabelDefaultWeight
  }

  "single undirected edge" should "have correct out-weight" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.outWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight)
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct out-weight for valid label" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have zero out-weight for invalid label" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(invalidEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  "single undirected edge" should "have default out-weight when label is valid, property is missing" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, missingPropertyOption, defaultWeight, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputValidLabelDefaultWeight
  }

  "single undirected edge" should "have default out-weight when property is missing" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.outWeight(vertexRDD, edgeRDD, missingPropertyOption, defaultWeight)
    results.collect().toSet shouldEqual expectedOutputValidLabelDefaultWeight
  }

  "single undirected edge" should "have correct undirected weight" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.undirectedWeightedDegree(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight)
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct undirected weight for valid label" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have zero undirected weight for invalid label" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(invalidEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  "single undirected edge" should "have default undirected-weight when label is valid, property is missing" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(vertexRDD, edgeRDD, missingPropertyOption, defaultWeight, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputValidLabelDefaultWeight
  }

  "single undirected edge" should "have default undirected-weight when property is missing" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.undirectedWeightedDegree(vertexRDD, edgeRDD, missingPropertyOption, defaultWeight)
    results.collect().toSet shouldEqual expectedOutputValidLabelDefaultWeight
  }

  "single undirected edge" should "have zero in-weight when restricted to empty set of edge labels" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.inWeightByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set()))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  "single undirected edge" should "have zero out-weight when restricted to empty set of edge labels" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set()))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  "single undirected edge" should "have zero undirected degree when restricted to empty set of edge labels" in new SingleUndirectedEdgeTest {
    val results = WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set()))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  trait SingleDirectedEdgeTest {

    val weightProperty = "waitwaitdon'ttellme"
    val weightPropertyOption = Some(weightProperty)
    val weight = 0.5D
    val missingProperty = "the missing"
    val missingPropertyOption = Some(missingProperty)
    val defaultWeight = 0.3D

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

  "single directed edge" should "have correct in-weight" in new SingleDirectedEdgeTest {
    val results = WeightedDegrees.inWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight)

    results.collect().toSet shouldEqual expectedOutputInDegreeValidLabel
  }

  "single directed edge" should "have correct in-weight for valid label" in new SingleDirectedEdgeTest {
    val results = WeightedDegrees.inWeightByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputInDegreeValidLabel
  }

  "single directed edge" should "have zero in-weight for invalid label" in new SingleDirectedEdgeTest {
    val results = WeightedDegrees.inWeightByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(invalidEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputInvalidLabel
  }

  "single directed edge" should "have default in-weight when label is valid, property is missing" in new SingleDirectedEdgeTest {
    val results = WeightedDegrees.inWeightByEdgeLabel(vertexRDD, edgeRDD, missingPropertyOption, defaultWeight, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputInDegreeDefault
  }

  "single directed edge" should "have default in-weight when property is missing" in new SingleDirectedEdgeTest {
    val results = WeightedDegrees.inWeight(vertexRDD, edgeRDD, missingPropertyOption, defaultWeight)
    results.collect().toSet shouldEqual expectedOutputInDegreeDefault
  }

  "single directed edge" should "have correct out-weight" in new SingleDirectedEdgeTest {
    val results = WeightedDegrees.outWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight)
    results.collect().toSet shouldEqual expectedOutputOutDegreeValidLabel
  }

  "single directed edge" should "have correct out-weight for valid label" in new SingleDirectedEdgeTest {
    val results = WeightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(validEdgeLabel)))

    results.collect().toSet shouldEqual expectedOutputOutDegreeValidLabel
  }

  "single directed edge" should "have zero out-weight for invalid label" in new SingleDirectedEdgeTest {
    val results = WeightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(invalidEdgeLabel)))

    results.collect().toSet shouldEqual expectedOutputInvalidLabel
  }

  "single directed edge" should "have default out-weight when label is valid, property is missing" in new SingleDirectedEdgeTest {
    val results = WeightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, missingPropertyOption, defaultWeight, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputOutDegreeDefault
  }

  "single directed edge" should "have default out-weight when property is missing" in new SingleDirectedEdgeTest {
    val results = WeightedDegrees.outWeight(vertexRDD, edgeRDD, missingPropertyOption, defaultWeight)
    results.collect().toSet shouldEqual expectedOutputOutDegreeDefault
  }

  trait BadGraphTest {

    val weightProperty = "waitwaitdon'ttellme"
    val weightPropertyOption = Some(weightProperty)
    val defaultWeight = 0D
    val weight = 0.5D

    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((4.toLong, 2L), (2L, 3L))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set.empty[Property])
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

  }

  "bad graph with mismatched edge and vertex RDDs" should "throw spark exception when computing out weight" in new BadGraphTest {
    intercept[org.apache.spark.SparkException] {
      val results = WeightedDegrees.outWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight).collect()
    }
  }

  "bad graph with mismatched edge and vertex RDDs" should "throw spark exception when computing in weight" in new BadGraphTest {
    intercept[org.apache.spark.SparkException] {
      val results = WeightedDegrees.inWeight(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight).collect()
    }

  }

  "bad graph with mismatched edge and vertex RDDs" should "throw spark exception when computing undirected weight" in new BadGraphTest {
    intercept[org.apache.spark.SparkException] {
      val results = WeightedDegrees.undirectedWeightedDegree(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight).collect()
    }
  }
}

