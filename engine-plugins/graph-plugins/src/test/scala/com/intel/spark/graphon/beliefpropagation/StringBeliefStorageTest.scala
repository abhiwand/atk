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

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.graphbuilder.elements.{ GBEdge, Property, GBVertex }
import org.apache.spark.rdd.RDD

/**
 * This test verifies that the priors and posteriors can be read and stored as comma delimited lists in properties.
 *
 * The test will be deprecated when this functionality is deprecated.
 */
class StringBeliefStorageTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

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
      stringOutput = Some(true),
      convergenceThreshold = None,
      posteriorProperty = propertyForLBPOutput)

  }

  "BeliefPropagationRunner with String Belief Storage" should "load and store properly with a two node disconnected graph" in new BPTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val pdfValues: Map[Long, String] = Map(1.toLong -> "\t1.0    0.0  ", 2.toLong -> "0.1, 0.9d \t")

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set()

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut = vertexSet.map(vid => GBVertex(vid, Property(vertexIdPropertyName, vid),
      Set(Property(inputPropertyName, pdfValues.get(vid).get), Property(propertyForLBPOutput, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = BeliefPropagationRunner.run(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val testIdsToStrings: Map[Long, String] = testVertices.map(gbVertex =>
      (gbVertex.physicalId.asInstanceOf[Long],
        gbVertex.getProperty(propertyForLBPOutput).get.value.asInstanceOf[String])).toMap

    val testBelief1Option = testIdsToStrings.get(1)
    val testBelief2Option = testIdsToStrings.get(2)

    val test = if (testBelief1Option.isEmpty || testBelief2Option.isEmpty) {
      false
    }
    else {
      val testBelief1 = testBelief1Option.get.split(",").map(s => s.toDouble)
      val testBelief2 = testBelief2Option.get.split(",").map(s => s.toDouble)

      (Math.abs(testBelief1.apply(0) - 1.0d) < floatingPointEqualityThreshold) &&
        (Math.abs(testBelief1.apply(1) - 0.0d) < floatingPointEqualityThreshold) &&
        (Math.abs(testBelief2.apply(0) - 0.1d) < floatingPointEqualityThreshold) &&
        (Math.abs(testBelief2.apply(1) - 0.9d) < floatingPointEqualityThreshold)
    }

    test shouldBe true
    testEdges shouldBe expectedEdgesOut
  }
}
