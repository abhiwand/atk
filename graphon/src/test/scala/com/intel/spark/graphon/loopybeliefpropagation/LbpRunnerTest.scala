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
    val propertyForLBPOutput = "LBP_VALUE"
    val expectedLBPValue = 0

    val vertexList: List[Long] = List(1, 2, 3, 5, 6, 7)

    //  directed edge list is made bidirectional with a flatmap

    val edgeList: List[(Long, Long)] = List((2.toLong, 6.toLong), (2.toLong, 4.toLong), (4.toLong, 6.toLong),
      (3.toLong, 5.toLong), (3.toLong, 7.toLong), (5.toLong, 7.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val gbVertexList = vertexList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Seq.empty[Property]))
    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Seq.empty[Property])
      })

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList)
  }

  "LBP Runner" should "properly update the vertex label but nothing else" in new LbpTest {

    val args = Lbp(graph = null, // we don't use this one in LbpRunner since we already have the RDDs for the graph
      vertex_value_property_list = None,
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

    val (verticesOut, edgesOut) = LbpRunner.runLbp(verticesIn, edgesIn, args)

    val testVertices = verticesOut.toArray().toList
    val testEdges = edgesOut.toArray().toList

    // no effect on the edge structure
    testEdges shouldBe gbEdgeList

    // vertices should have had their properties updated

    val expectedVerticesOut =
      vertexList.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Seq(Property(propertyForLBPOutput, expectedLBPValue))))

    testVertices shouldBe expectedVerticesOut
  }
}