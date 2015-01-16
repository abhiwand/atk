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
 * Exercises the degree calculation utilities on trivial and malformed graphs.
 */
class DegreeCornerCasesTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val defaultParallelism = 3 // use of value > 1 to catch stupid parallelization bugs

  "empty graph" should "result in empty results" in {
    val vertexRDD = sparkContext.parallelize(List.empty[GBVertex], defaultParallelism)
    val edgeRDD = sparkContext.parallelize(List.empty[GBEdge], defaultParallelism)

    GraphStatistics.outDegrees(vertexRDD, edgeRDD).count() shouldBe 0
    GraphStatistics.outDegreesByEdgeLabel(vertexRDD, edgeRDD, "edge label").count() shouldBe 0
    GraphStatistics.inDegrees(vertexRDD, edgeRDD).count() shouldBe 0
    GraphStatistics.inDegreesByEdgeLabel(vertexRDD, edgeRDD, "edge label").count() shouldBe 0
  }

    "single node graph" should "have all edge labels degree 0" in {

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
        gbVertexList.map(v => (v, 0.toLong)).toSet

      GraphStatistics.outDegrees(vertexRDD, edgeRDD).collect().toSet shouldBe expectedOutput
      GraphStatistics.outDegreesByEdgeLabel(vertexRDD, edgeRDD, validEdgeLabel).collect().toSet shouldBe expectedOutput
      GraphStatistics.inDegrees(vertexRDD, edgeRDD).collect().toSet shouldBe expectedOutput
      GraphStatistics.inDegreesByEdgeLabel(vertexRDD, edgeRDD, validEdgeLabel).collect().toSet shouldBe expectedOutput
    }



  trait SingleUndirectedEdgeTest {
    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((1.toLong, 2.toLong), (2.toLong, 1.toLong))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set.empty[Property])
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

    private val invalidDegrees: Map[Long, Long] = Map(1.toLong -> 0.toLong, 2.toLong -> 0.toLong)
    private val validDegrees: Map[Long, Long] = Map(1.toLong -> 1.toLong, 2.toLong -> 1.toLong)

    val expectedOutputValidLabel = gbVertexList.map(v => (v, validDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputInvalidLabel = gbVertexList.map(v => (v, invalidDegrees(v.physicalId.asInstanceOf[Long]))).toSet
  }

  "single undirected edge" should "have correct in-degree" in new SingleUndirectedEdgeTest {
    val results = GraphStatistics.inDegrees(vertexRDD, edgeRDD)

    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct in-degree for valid label" in new SingleUndirectedEdgeTest {
    val results = GraphStatistics.inDegreesByEdgeLabel(vertexRDD, edgeRDD, validEdgeLabel)
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct in-degree for invalid label" in new SingleUndirectedEdgeTest {
    val results = GraphStatistics.inDegreesByEdgeLabel(vertexRDD, edgeRDD, invalidEdgeLabel)
    results.collect().toSet shouldEqual expectedOutputInvalidLabel
  }

  "single undirected edge" should "have correct out-degree" in new SingleUndirectedEdgeTest {
    val results = GraphStatistics.outDegrees(vertexRDD, edgeRDD)

    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct out-degree for valid label" in new SingleUndirectedEdgeTest {
    val results = GraphStatistics.outDegreesByEdgeLabel(vertexRDD, edgeRDD, validEdgeLabel)

    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct out-degree for invalid label" in new SingleUndirectedEdgeTest {
    val results = GraphStatistics.outDegreesByEdgeLabel(vertexRDD, edgeRDD, invalidEdgeLabel)

    results.collect().toSet shouldEqual expectedOutputInvalidLabel
  }

  trait SingleDirectedEdgeTest {
    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((1.toLong, 2.toLong))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set.empty[Property])
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

    private val invalidDegrees: Map[Long, Long] = Map(1.toLong -> 0.toLong, 2.toLong -> 0.toLong)
    private val validInDegrees: Map[Long, Long] = Map(1.toLong -> 0.toLong, 2.toLong -> 1.toLong)
    private val validOutDegrees: Map[Long, Long] = Map(1.toLong -> 1.toLong, 2.toLong -> 0.toLong)

    val expectedOutputInDegreeValidLabel =
      gbVertexList.map(v => (v, validInDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputOutDegreeValidLabel =
      gbVertexList.map(v => (v, validOutDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputInvalidLabel = gbVertexList.map(v => (v, invalidDegrees(v.physicalId.asInstanceOf[Long]))).toSet
  }

  "single directed edge" should "have correct in-degree" in new SingleDirectedEdgeTest {
    val results = GraphStatistics.inDegrees(vertexRDD, edgeRDD)

    results.collect().toSet shouldEqual expectedOutputInDegreeValidLabel
  }

  "single directed edge" should "have correct in-degree for valid label" in new SingleDirectedEdgeTest {
    val results = GraphStatistics.inDegreesByEdgeLabel(vertexRDD, edgeRDD, validEdgeLabel)
    results.collect().toSet shouldEqual expectedOutputInDegreeValidLabel
  }

  "single directed edge" should "have correct in-degree for invalid label" in new SingleDirectedEdgeTest {
    val results = GraphStatistics.inDegreesByEdgeLabel(vertexRDD, edgeRDD, invalidEdgeLabel)
    results.collect().toSet shouldEqual expectedOutputInvalidLabel
  }

  "single directed edge" should "have correct out-degree" in new SingleDirectedEdgeTest {
    val results = GraphStatistics.outDegrees(vertexRDD, edgeRDD)

    results.collect().toSet shouldEqual expectedOutputOutDegreeValidLabel
  }

  "single directed edge" should "have correct out-degree for valid label" in new SingleDirectedEdgeTest {
    val results = GraphStatistics.outDegreesByEdgeLabel(vertexRDD, edgeRDD, validEdgeLabel)

    results.collect().toSet shouldEqual expectedOutputOutDegreeValidLabel
  }

  "single directed edge" should "have correct out-degree for invalid label" in new SingleDirectedEdgeTest {
    val results = GraphStatistics.outDegreesByEdgeLabel(vertexRDD, edgeRDD, invalidEdgeLabel)

    results.collect().toSet shouldEqual expectedOutputInvalidLabel
  }

  trait BadGraphTest {
    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((4.toLong, 2.toLong), (2.toLong, 3.toLong))

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

  "bad graph with mismatched edge and vertex RDDs" should "throw spark exception when computing out degrees" in new BadGraphTest {
    intercept[org.apache.spark.SparkException] {
      val results = GraphStatistics.outDegrees(vertexRDD, edgeRDD).collect()
    }
  }

  "bad graph with mismatched edge and vertex RDDs" should "throw spark exception when computing in degrees" in new BadGraphTest {
    intercept[org.apache.spark.SparkException] {
      val results = GraphStatistics.inDegrees(vertexRDD, edgeRDD).collect()
    }
  }
}

