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
    val expectedLBPValue  = new Array[Double](0)

    val vertexSet: Set[Long] = Set(1, 2, 3, 4, 5, 6, 7)

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((2.toLong, 6.toLong), (2.toLong, 4.toLong), (4.toLong, 6.toLong),
      (3.toLong, 5.toLong), (3.toLong, 7.toLong), (5.toLong, 7.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Seq.empty[Property]))
    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Seq.empty[Property])
      })

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)
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

    val testVertices = verticesOut.toArray().toSet
    val testEdges = edgesOut.toArray().toSet

    // no effect on the edge structure
    testEdges shouldBe gbEdgeSet

    // vertices should have had their properties updated

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Seq(Property(propertyForLBPOutput, expectedLBPValue))))

    testVertices.map({case gbVertex: GBVertex => gbVertex.physicalId }) shouldBe expectedVerticesOut.map({case gbVertex: GBVertex => gbVertex.physicalId })
    testVertices.map({case gbVertex: GBVertex => gbVertex.gbId }) shouldBe expectedVerticesOut.map({case gbVertex: GBVertex => gbVertex.gbId })
    testVertices.forall({case gbVertex: GBVertex => ( gbVertex.getProperty(propertyForLBPOutput).get.value.asInstanceOf[Array[Double]].isEmpty)}) shouldBe true
    testVertices.forall({case gbVertex: GBVertex => ( gbVertex.properties.length == 1)}) shouldBe true
  }
}