package com.intel.spark.graphon.loopybeliefpropagation

import org.scalatest.Matchers

import java.util.Date
import org.apache.spark.SparkContext
import scala.concurrent.Lock
import org.scalatest.{ FlatSpec, BeforeAndAfter }
import org.apache.log4j.{ Logger, Level }
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.spark.graphon.connectedcomponents.NormalizeConnectedComponents
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.graph.GraphReference
import scala.reflect.internal.util.StringOps

class LbpRunnerTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait LbpTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"


  }

  "LBP Runner" should "work properly with a two node graph, unit probabilities" in new LbpTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val pdfValues: Map[Long, List[Double]] = Map(1.toLong -> List(1.0d, 0.0d),
      2.toLong -> List(1.0d, 0.0d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

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

    testVertices shouldEqual expectedVerticesOut
    testEdges shouldBe expectedEdgesOut

  }

  "LBP Runner" should "work properly with a two node graph, equal, non-unit probabilities" in new LbpTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val pdfValues: Map[Long, List[Double]] = Map(1.toLong -> List(0.3d, 0.7d),
      2.toLong -> List(0.3d, 0.7d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

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

    testVertices shouldEqual expectedVerticesOut
    testEdges shouldBe expectedEdgesOut

  }
}