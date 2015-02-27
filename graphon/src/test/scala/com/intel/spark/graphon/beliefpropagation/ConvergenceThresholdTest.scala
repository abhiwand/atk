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
 * "Convergence threshold" in our system:
 * When the average change in posterior beliefs between supersteps falls below this threshold,
 * terminate. Terminate! TERMINATE!!!
 *
 */
class ConvergenceThresholdTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait CTTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"

    val floatingPointEqualityThreshold: Double = 0.000000001d

    val vertexSet: Set[Long] = Set(1, 2)

    val firstNodePriors = Vector(0.6d, 0.4d)
    val secondNodePriors = Vector(0.3d, 0.7d)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> firstNodePriors,
      2.toLong -> secondNodePriors)

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, priors.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

  }

  "BP Runner" should "run for one iteration when convergence threshold is 1.0" in new CTTest {

    val args = BeliefPropagationRunnerArgs(
      priorProperty = inputPropertyName,
      edgeWeightProperty = None,
      maxIterations = Some(10),
      stringOutput = None,
      convergenceThreshold = Some(1d),
      posteriorProperty = propertyForLBPOutput)

    val (verticesOut, edgesOut, log) = BeliefPropagationRunner.run(verticesIn, edgesIn, args)

    log should include("Total number of iterations: 1")
  }

  "BP Runner" should "run for two iterations when convergence threshold is 0.2" in new CTTest {

    val args = BeliefPropagationRunnerArgs(
      priorProperty = inputPropertyName,
      edgeWeightProperty = None,
      maxIterations = Some(10),
      stringOutput = None,
      convergenceThreshold = Some(0.2d),
      posteriorProperty = propertyForLBPOutput)

    val (verticesOut, edgesOut, log) = BeliefPropagationRunner.run(verticesIn, edgesIn, args)

    log should include("Total number of iterations: 2")
  }

  "BP Runner" should "run for two iterations when  convergence threshold is 0" in new CTTest {

    val args = BeliefPropagationRunnerArgs(
      priorProperty = inputPropertyName,
      edgeWeightProperty = None,
      maxIterations = Some(10),
      stringOutput = None,
      convergenceThreshold = Some(0d),
      posteriorProperty = propertyForLBPOutput)

    val (verticesOut, edgesOut, log) = BeliefPropagationRunner.run(verticesIn, edgesIn, args)

    log should include("Total number of iterations: 2")
  }

  // an example that slowly converges to an asymptote would make a better test when no threshold is given

  "BP Runner" should "run for two iterations when no convergence threshold given" in new CTTest {

    val args = BeliefPropagationRunnerArgs(
      priorProperty = inputPropertyName,
      edgeWeightProperty = None,
      maxIterations = Some(10),
      stringOutput = None,
      convergenceThreshold = None,
      posteriorProperty = propertyForLBPOutput)

    val (verticesOut, edgesOut, log) = BeliefPropagationRunner.run(verticesIn, edgesIn, args)

    log should include("Total number of iterations: 2")
  }
}
