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

package com.intel.spark.graphon.beliefpropagation

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.testutils.ApproximateVertexEquality

/**
 * These tests make sure that belief propagation can correctly handle graphs with no edges and even graphs with
 * no vertices. It sounds silly till the thing crashes on you.
 */
class DegenerateCasesTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait BPTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"

    val floatingPointEqualityThreshold: Double = 0.000000001d

    val args = BeliefPropagationRunnerArgs(
      priorProperty = inputPropertyName,
      edgeWeightProperty = None,
      maxIterations = Some(10),
      stringOutput = None,
      convergenceThreshold = None,
      posteriorProperty = propertyForLBPOutput)

  }

  "BP Runner" should "work properly with an empty graph" in new BPTest {

    val vertexSet: Set[Long] = Set()

    val pdfValues: Map[Long, Vector[Double]] = Map()

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set()

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, pdfValues.get(vid).get),
          Property(propertyForLBPOutput, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = BeliefPropagationRunner.run(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    testVertices shouldEqual expectedVerticesOut
    testEdges shouldBe expectedEdgesOut

  }

  "BP Runner" should "work properly with a single node graph" in new BPTest {

    val vertexSet: Set[Long] = Set(1)

    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(0.550d, 0.45d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set()

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, pdfValues.get(vid).get),
          Property(propertyForLBPOutput, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = BeliefPropagationRunner.run(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    testVertices shouldEqual expectedVerticesOut
    testEdges shouldBe expectedEdgesOut
  }

  "BP Runner" should "work properly with a two node disconnected graph" in new BPTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d, 0.0d),
      2.toLong -> Vector(0.1d, 0.9d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set()

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, pdfValues.get(vid).get),
          Property(propertyForLBPOutput, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = BeliefPropagationRunner.run(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.approximatelyEquals(testVertices,
      expectedVerticesOut,
      List(propertyForLBPOutput),
      floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut
  }

}
