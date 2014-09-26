package com.intel.spark.graphon.loopybeliefpropagation

import org.apache.spark.rdd.RDD
import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import com.intel.spark.graphon.testutils.ApproximateVertexEquality

/**
 * These tests validate loopy belief propagation on two node graphs.
 *
 * These provide simple examples for detecting errors in message calculation and in belief readout.
 */
class TwoNodeTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait LbpTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"

    val floatingPointEqualityThreshold: Double = 0.000000001d

    val args = Lbp(graph = null, // we don't use this one in LbpRunner since we already have the RDDs for the graph
      vertexPriorPropertyName = inputPropertyName,
      edgeWeightProperty = None,
      posteriorPropertyName = propertyForLBPOutput,
      maxSuperSteps = None)

  }

  "LBP Runner" should "work with two nodes of differing unit states" in new LbpTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d, 0.0d), 2.toLong -> Vector(0.0d, 1.0d))
    val expectedPosteriors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d, 0.0d), 2.toLong -> Vector(0.0d, 1.0d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set()

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, priors.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, expectedPosteriors.get(vid).get),
          Property(propertyForLBPOutput, priors.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = LbpRunner.runLbp(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.approximatelyEquals(testVertices,
      expectedVerticesOut,
      List(propertyForLBPOutput),
      floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut

  }

  "LBP Runner" should "work properly with a two node graph, uniform probabilities" in new LbpTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(0.5d, 0.5d),
      2.toLong -> Vector(0.5d, 0.5d))

    val expectedPosteriors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(0.5d, 0.5d),
      2.toLong -> Vector(0.5d, 0.5d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

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

    val (verticesOut, edgesOut, log) = LbpRunner.runLbp(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.approximatelyEquals(testVertices,
      expectedVerticesOut,
      List(propertyForLBPOutput),
      floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut

  }

  "LBP Runner" should "work properly with one node uniform, one node unit" in new LbpTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d, 0.0d),
      2.toLong -> Vector(0.5d, 0.5d))

    val expectedPosteriors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d, 0.0d),
      2.toLong -> Vector(Math.E / (Math.E + 1), 1 / (Math.E + 1)))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

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

    val (verticesOut, edgesOut, log) = LbpRunner.runLbp(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.approximatelyEquals(testVertices,
      expectedVerticesOut,
      List(propertyForLBPOutput),
      floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut

  }

  "LBP Runner" should "work properly with two nodes of differing non-uniform, non-unit priors" in new LbpTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val firstNodePriors = Vector(0.6d, 0.4d)
    val secondNodePriors = Vector(0.3d, 0.7d)

    val messageFirstToSecond = Vector(firstNodePriors.head + firstNodePriors.last / Math.E,
      (firstNodePriors.head / Math.E) + firstNodePriors.last)

    val messageSecondToFirst = Vector(secondNodePriors.head + secondNodePriors.last / Math.E,
      (secondNodePriors.head / Math.E) + secondNodePriors.last)

    val unnormalizedBeliefsFirstNode: Vector[Double] = firstNodePriors.zip(messageSecondToFirst).map({ case (p, m) => p * m })
    val unnormalizedBeliefsSecondNode: Vector[Double] = secondNodePriors.zip(messageFirstToSecond).map({ case (p, m) => p * m })

    val expectedFirstNodePosteriors = unnormalizedBeliefsFirstNode.map(x => x / unnormalizedBeliefsFirstNode.reduce(_ + _))
    val expectedSecondNodePosteriors = unnormalizedBeliefsSecondNode.map(x => x / unnormalizedBeliefsSecondNode.reduce(_ + _))

    expectedFirstNodePosteriors shouldEqual Vector(0.5078674222109657d, 0.4921325777890343d)
    expectedSecondNodePosteriors shouldEqual Vector(0.3403080027827025d, 0.6596919972172975d)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> firstNodePriors,
      2.toLong -> secondNodePriors)

    val expectedPosteriors: Map[Long, Vector[Double]] = Map(1.toLong -> expectedFirstNodePosteriors,
      2.toLong -> expectedSecondNodePosteriors)

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

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

    val (verticesOut, edgesOut, log) = LbpRunner.runLbp(verticesIn, edgesIn, args)

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
