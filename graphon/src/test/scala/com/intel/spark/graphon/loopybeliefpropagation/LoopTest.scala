package com.intel.spark.graphon.loopybeliefpropagation

import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.testutils.ApproximateVertexEquality

/**
 * These test cases validate that LBP works correctly on (very simple) graphs that contain loops.
 */
class LoopTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait LbpTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"

    val floatingPointEqualityThreshold : Double = 0.000000001d

  }
  "LBP Runner" should "work with a triangle with uniform probabilities" in new LbpTest {

    val vertexSet: Set[Long] = Set(1, 2, 3)

    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(0.5d, 0.5d),
      2.toLong -> Vector(0.5d, 0.5d), 3.toLong -> Vector(0.5d, 0.5d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong), (1.toLong, 3.toLong), (2.toLong, 3.toLong))
      .flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, pdfValues.get(vid).get),
          Property(propertyForLBPOutput, pdfValues.get(vid).get))))

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
      max_supersteps = None,
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

  "LBP Runner" should "work with a four-cycle with uniform probabilities" in new LbpTest {

    val vertexSet: Set[Long] = Set(1, 2, 3, 4)

    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(0.5d, 0.5d),
      2.toLong -> Vector(0.5d, 0.5d), 3.toLong -> Vector(0.5d, 0.5d), 4.toLong -> Vector(0.5d, 0.5d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong), (2.toLong, 3.toLong),
      (3.toLong, 4.toLong), (4.toLong, 1.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, pdfValues.get(vid).get),
          Property(propertyForLBPOutput, pdfValues.get(vid).get))))

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
      max_supersteps = None,
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
