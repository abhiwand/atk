package com.intel.spark.graphon.loopybeliefpropagation

import com.intel.graphbuilder.elements.{ Edge, Property, Vertex }
import org.apache.spark.rdd.RDD
import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import com.intel.spark.graphon.testutils.ApproximateVertexEquality

/**
 * For graphs that are trees, belief propagation is known to converge to the exact solution with a number of iterations
 * bounded by the diameter of the graph.
 *
 * These tests validate that our LBP arrives at the correct answer for some small trees.
 */
class TreesTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait LbpTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"

    val floatingPointEqualityThreshold: Double = 0.000000001d

  }

  "LBP Runner" should "work properly on a five node tree with degree sequence 1, 1, 3, 2, 1" in new LbpTest {

    val vertexSet: Set[Long] = Set(1, 2, 3, 4, 5)

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 3.toLong), (2.toLong, 3.toLong), (3.toLong, 4.toLong),
      (4.toLong, 5.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val firstNodePriors = Vector(0.1d, 0.9d)
    val secondNodePriors = Vector(0.2d, 0.8d)
    val thirdNodePriors = Vector(0.3d, 0.7d)
    val fourthNodePriors = Vector(0.4d, 0.6d)
    val fifthNodePriors = Vector(0.5d, 0.5d)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> firstNodePriors, 2.toLong -> secondNodePriors,
      3.toLong -> thirdNodePriors, 4.toLong -> fourthNodePriors, 5.toLong -> fifthNodePriors)

    // messages that converge after the first round - that is,  the constant ones coming from the leaves

    val message1to3 = Vector(firstNodePriors.head + firstNodePriors.last / Math.E,
      firstNodePriors.head / Math.E + firstNodePriors.last)

    val message2to3 = Vector(secondNodePriors.head + secondNodePriors.last / Math.E,
      secondNodePriors.head / Math.E + secondNodePriors.last)

    val message5to4 = Vector(fifthNodePriors.head + fifthNodePriors.last / Math.E,
      fifthNodePriors.head / Math.E + fifthNodePriors.last)

    // messages the converge after the second round, after the leaves have reported

    val message3to4 = Vector(thirdNodePriors.head * message1to3.head * message2to3.head
      + (1 / Math.E) * thirdNodePriors.last * message1to3.last * message2to3.last,
      (1 / Math.E) * thirdNodePriors.head * message1to3.head * message2to3.head
        + thirdNodePriors.last * message1to3.last * message2to3.last
    )

    val message4to3 = Vector(fourthNodePriors.head * message5to4.head
      + (1 / Math.E) * fourthNodePriors.last * message5to4.last,
      (1 / Math.E) * fourthNodePriors.head * message5to4.head
        + fourthNodePriors.last * message5to4.last
    )

    // messages converging in the third round

    val message4to5 = Vector(fourthNodePriors.head * message3to4.head
      + (1 / Math.E) * fourthNodePriors.last * message3to4.last,
      (1 / Math.E) * fourthNodePriors.head * message3to4.head
        + fourthNodePriors.last * message3to4.last
    )

    val message3to1 = Vector(
      thirdNodePriors.head * message4to3.head * message2to3.head
        + 1 / Math.E * thirdNodePriors.last * message4to3.last * message2to3.last,
      1 / Math.E * thirdNodePriors.head * message4to3.head * message2to3.head
        + thirdNodePriors.last * message4to3.last * message2to3.last
    )

    val message3to2 = Vector(
      thirdNodePriors.head * message4to3.head * message1to3.head
        + 1 / Math.E * thirdNodePriors.last * message4to3.last * message1to3.last,
      1 / Math.E * thirdNodePriors.head * message4to3.head * message1to3.head
        + thirdNodePriors.last * message4to3.last * message1to3.last
    )

    // calculate expected posteriors

    val expectedPosterior1 = VectorMath.l1Normalize(VectorMath.overflowProtectedProduct(firstNodePriors, message3to1))
    val expectedPosterior2 = VectorMath.l1Normalize(VectorMath.overflowProtectedProduct(secondNodePriors, message3to2))
    val expectedPosterior3 = VectorMath.l1Normalize(VectorMath.overflowProtectedProduct(List(thirdNodePriors, message1to3, message2to3, message4to3)))
    val expectedPosterior4 = VectorMath.l1Normalize(VectorMath.overflowProtectedProduct(List(fourthNodePriors, message3to4, message5to4)))
    val expectedPosterior5 = VectorMath.l1Normalize(VectorMath.overflowProtectedProduct(fifthNodePriors, message4to5))

    val expectedPosteriors: Map[Long, Vector[Double]] = Map(1.toLong -> expectedPosterior1, 2.toLong -> expectedPosterior2,
      3.toLong -> expectedPosterior3, 4.toLong -> expectedPosterior4, 5.toLong -> expectedPosterior5)

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, priors.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, priors.get(vid).get),
          Property(propertyForLBPOutput, expectedPosteriors.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val args = Lbp(graph = null, // we don't use this one in LbpRunner since we already have the RDDs for the graph
      vertex_value_property_list = Some(inputPropertyName),
      edge_value_property_list = None,
      input_edge_label_list = None,
      output_vertex_property_list = Some(propertyForLBPOutput),
      vertex_type_property_key = None,
      vector_value = None,
      max_supersteps = Some(4),
      convergence_threshold = None,
      anchor_threshold = None,
      smoothing = None,
      bidirectional_check = None,
      ignore_vertex_type = None,
      max_product = None,
      power = None)

    val (verticesOut, edgesOut, log) = LbpRunner.runLbp(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.equalsApproximateAtProperty(testVertices,
      expectedVerticesOut,
      propertyForLBPOutput,
      floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut
  }
}
