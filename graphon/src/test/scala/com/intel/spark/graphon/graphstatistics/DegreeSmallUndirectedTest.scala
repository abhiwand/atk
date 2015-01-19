package com.intel.spark.graphon.graphstatistics

import com.intel.graphbuilder.elements.{ GBEdge, Property, GBVertex }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Tests the calculation of undirected degrees on a simple undirected graph.
 *
 * The graph has 5 vertices and two edge relations, "A edges" and "B edges"
 *
 * The vertex set is 1, 2, 3, 4, 5.
 *
 * All edges are undirected.
 *
 * The A edges are:  12, 13, 23, 34
 * The B edges are: 15, 34, 35, 45
 *
 */
class DegreeSmallUndirectedTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val defaultParallelism = 3 // use of parellelism > 1 to catch stupid parallelization bugs

  trait UndirectedGraphTest {

    val edgeLabelA = "A"
    val edgeLabelB = "B"

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1L, 2L, 3L, 4L, 5L)

    val edgeListA: List[(Long, Long)] = List((1L, 3L), (1L, 2L), (2L, 3L), (3L, 4L)).
      flatMap({ case (x, y) => Set((x, y), (y, x)) })
    val aDegreeMap: Map[Long, Long] = Map((1L, 2L), (2L, 2L), (3L, 3L), (4L, 1L), (5L, 0L))

    val edgeListB: List[(Long, Long)] = List((1L, 5L), (3L, 5L), (3L, 4L), (4L, 5L)).
      flatMap({ case (x, y) => Set((x, y), (y, x)) })
    val bDegreeMap: Map[Long, Long] = Map((1L, 1L), (2L, 0L), (3L, 2L), (4L, 2L), (5L, 3L))

    val combinedDegreesMap: Map[Long, Long] = Map((1L, 3L), (2L, 2L), (3L, 5L), (4L, 3L), (5L, 3L))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeListA =
      edgeListA.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            edgeLabelA, Set.empty[Property])
      })

    val gbEdgeListB =
      edgeListB.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            edgeLabelB, Set.empty[Property])
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeListA.union(gbEdgeListB))

    val excpectedADegrees: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, aDegreeMap(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedBDegrees: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, bDegreeMap(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedCombinedDegrees: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, combinedDegreesMap(v.physicalId.asInstanceOf[Long]))).toSet

    val allZeroDegrees: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, 0L)).toSet
  }

  "simple undirected graph" should "have correct degree for edge label A" in new UndirectedGraphTest {
    val results = DegreeStatistics.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Set(edgeLabelA))
    results.collect().toSet shouldEqual excpectedADegrees
  }

  "simple undirected graph" should "have correct degree for edge label B" in new UndirectedGraphTest {
    val results = DegreeStatistics.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Set(edgeLabelB))
    results.collect().toSet shouldEqual expectedBDegrees
  }

  "simple undirected graph" should "have correct degree when both edge label A and B requested" in new UndirectedGraphTest {
    val results = DegreeStatistics.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Set(edgeLabelA, edgeLabelB))
    results.collect().toSet shouldEqual expectedCombinedDegrees
  }

  "simple undirected graph" should "have net degrees for all labels combined when edge labels are unrestricted " in new UndirectedGraphTest {
    val results = DegreeStatistics.undirectedDegrees(vertexRDD, edgeRDD)
    results.collect().toSet shouldEqual expectedCombinedDegrees
  }

  "simple undirected graph" should "have all degrees 0 when restricted to empty list of labels" in new UndirectedGraphTest {
    val results = DegreeStatistics.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Set())
    results.collect().toSet shouldEqual allZeroDegrees
  }
}
