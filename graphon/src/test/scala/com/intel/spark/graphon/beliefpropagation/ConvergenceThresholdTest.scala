package com.intel.spark.graphon.beliefpropagation

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.graphbuilder.elements.{ Edge, Property, Vertex }
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.testutils.ApproximateVertexEquality
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }

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
          GBEdge(src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
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
