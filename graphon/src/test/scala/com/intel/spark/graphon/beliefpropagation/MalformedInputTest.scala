package com.intel.spark.graphon.beliefpropagation

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.graphbuilder.elements.{ Edge => GBEdge, Property, Vertex => GBVertex }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException

/**
 * Tests that check that bad inputs and arguments will raise the appropriate exceptions.
 */

class MalformedInputTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait BPTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"

    val floatingPointEqualityThreshold: Double = 0.000000001d

    val args = BeliefPropagationArgs(graph = null, // we don't use this one in LbpRunner since we already have the RDDs for the graph
      priorProperty = inputPropertyName,
      stateSpaceSize = 2,
      edgeWeightProperty = None,
      maxIterations = Some(10),
      stringOutput = None,
      posteriorProperty = propertyForLBPOutput)

  }

  "BP Runner" should "throw an illegal argument exception when an input vector length does not match the size of the state space " in new BPTest {

    val vertexSet: Set[Long] = Set(1)

    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set()

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

    // This on Spark, so the IllegalArgumentException bubbles up through a SparkException
    val exception = intercept[SparkException] {
      val (verticesOut, edgesOut, log) = BeliefPropagationRunner.run(verticesIn, edgesIn, args)
    }

    exception.asInstanceOf[SparkException].getMessage should include("IllegalArgumentException")
    exception.asInstanceOf[SparkException].getMessage should include("Length of prior does not match state space size")
  }

  "BP Runner" should "throw a NotFoundException when the vertex does provide the request property" in new BPTest {

    val vertexSet: Set[Long] = Set(1)

    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set()

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property("hahawrongname", pdfValues.get(x).get))))

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

    // This on Spark, so the IllegalArgumentException bubbles up through a SparkException
    val exception = intercept[SparkException] {
      BeliefPropagationRunner.run(verticesIn, edgesIn, args)
    }

    exception.asInstanceOf[SparkException].getMessage should include("NotFoundException")
    exception.asInstanceOf[SparkException].getMessage should include(inputPropertyName)
  }
}