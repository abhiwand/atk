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

class LbpRunnerTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait LbpTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"
    val expectedLBPValue = new Array[Double](0)

    val vertexSet: Set[Long] = Set(1, 2, 3, 4, 5, 6, 7)

    val pdfValues: Map[Long, Array[Double]] = Map(1.toLong -> Array(0.1d, 0.9d),
      2.toLong -> Array(0.2d, 0.8d),
      3.toLong -> Array(0.3d, 0.7d),
      4.toLong -> Array(0.4d, 0.6d),
      5.toLong -> Array(0.5d, 0.5d),
      6.toLong -> Array(0.6d, 0.4d),
      7.toLong -> Array(0.7d, 0.3d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((2.toLong, 6.toLong), (2.toLong, 4.toLong), (4.toLong, 6.toLong),
      (3.toLong, 5.toLong), (3.toLong, 7.toLong), (5.toLong, 7.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))
    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)
  }

  "LBP Runner" should "properly update the vertex property specified but nothing else" in new LbpTest {

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

    val gbV1 = GBVertex(666, Property(vertexIdPropertyName, 666), Set(Property("heynow", "therayago"), Property("foo", 10)))
    val gbV2 = GBVertex(666, Property(vertexIdPropertyName, 666), Set(Property("foo", 10), Property("heynow", "therayago")))

    gbV1 shouldEqual gbV2

    val (verticesOut, edgesOut, log) = LbpRunner.runLbp(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    // no effect on the edge structure
    testEdges shouldBe gbEdgeSet

    // vertices should have had their properties updated

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(propertyForLBPOutput, expectedLBPValue))))

    testVertices.map({ case gbVertex: GBVertex => gbVertex.physicalId }) shouldBe expectedVerticesOut.map({ case gbVertex: GBVertex => gbVertex.physicalId })
    testVertices.map({ case gbVertex: GBVertex => gbVertex.gbId }) shouldBe expectedVerticesOut.map({ case gbVertex: GBVertex => gbVertex.gbId })
    testVertices.forall({ case gbVertex: GBVertex => (gbVertex.properties.size == 2) }) shouldBe true

    //   val testIdsToArrays = testVertices.map({ case gbVertex: GBVertex => (gbVertex.physicalId, gbVertex.getProperty(inputPropertyName).get.asInstanceOf[Array[Double]]) }).toMap

    // vertexSet.forall(i => (pdfValues.get(i).get equals testIdsToArrays.get(i).get)) shouldBe true

    // testVertices.forall({ case gbVertex: GBVertex => (gbVertex.getProperty(propertyForLBPOutput).get.value.asInstanceOf[Array[Double]].isEmpty) }) shouldBe true
  }
}