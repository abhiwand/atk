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
import com.intel.spark.graphon.testutils.ApproximateVertexEquality
import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }

/**
 * This test makes sure that we do not get underflow errors which cause some posteriors to become all zero vectors.
 *
 */
class UnderFlowTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait UFTest {

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

  "BP Runner" should "not have any all 0 posteriors" in new UFTest {

    // it's a 3x3 torus

    val vertexSet: Set[Long] = Set(1, 2, 3, 4, 5, 6, 7, 8, 9)

    val edgeSet: Set[(Long, Long)] = Set((1, 2), (1, 4), (2, 3), (2, 5), (3, 1), (3, 6),
      (4, 5), (4, 7), (5, 6), (5, 8), (6, 4), (6, 9), (7, 8), (7, 1), (8, 9), (8, 2), (9, 7), (9, 3)).flatMap({ case (x, y) => Set((x.toLong, y.toLong), (y.toLong, x.toLong)) })

    val prior = Vector(0.9d, 0.1d)

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, prior))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = BeliefPropagationRunner.run(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet

    def vectorStrictlyPositive(v: Vector[Double]) = (v.forall(x => x >= 0d)) && (v.exists(x => x > 0d))

    val test = testVertices.forall(v => vectorStrictlyPositive(v.getProperty(propertyForLBPOutput).get.value.asInstanceOf[Vector[Double]]))

    test shouldBe true
  }
}
