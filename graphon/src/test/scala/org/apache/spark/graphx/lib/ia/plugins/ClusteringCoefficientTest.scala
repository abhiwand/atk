//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package org.apache.spark.graphx.lib.ia.plugins

import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.graphx
import org.apache.spark.graphx.{ Graph, PartitionStrategy }
import org.scalatest.{ FlatSpec, Matchers }

/**
 * "Convergence threshold" in our system:
 * When the average change in posterior beliefs between supersteps falls below this threshold,
 * terminate. Terminate! TERMINATE!!!
 *
 */
class ClusteringCoefficientTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val floatingPointEqualityThreshold: Double = 0.000000001d
  val defaultParallelism = 3 // > 1 to catch stupid parallelization bugs

  "clustering coefficient" should "not crash (and produce an empty graph) when run on an empty graph" in {

    val rawEdges = sparkContext.parallelize(Array[(Long, Long)](), defaultParallelism)
    val graph = Graph.fromEdgeTuples(rawEdges, true).partitionBy(PartitionStrategy.RandomVertexCut).cache()
    val (testGraph, testGCC) = ClusteringCoefficient.run(graph)
    val testVertices = testGraph.vertices.collect()
    val testEdges = testGraph.edges.collect()

    testVertices.length shouldBe 0
    testEdges.length shouldBe 0
  }

  "clustering coefficient" should "should correctly give an isolated vertex clustering coefficient 0.0d" in {

    val vertex = sparkContext.parallelize(Array((1L, null)), defaultParallelism)
    val edges = sparkContext.parallelize(Array[graphx.Edge[Null]](), defaultParallelism)
    val graph = Graph(vertex, edges).partitionBy(PartitionStrategy.RandomVertexCut) cache ()
    val (testGraph, testGCC) = ClusteringCoefficient.run(graph)
    val testVertices = testGraph.vertices.collect()
    val testEdges = testGraph.edges.collect()

    testEdges.length shouldBe 0
    testVertices.length shouldBe 1

    val testVertexClusteringCoefficient: Double = testVertices(0)._2

    Math.abs(testVertexClusteringCoefficient - 0.0d) should be < floatingPointEqualityThreshold
    Math.abs(testGCC - 0.0d) should be < floatingPointEqualityThreshold
  }

  "clustering coefficient" should "give every vertex a CC 1.0d when analyzing a complete graph" in {

    val edges = sparkContext.parallelize(Array[(Long, Long)]((1L -> 2L), (1L -> 3L), (1L -> 4L), (2L -> 3L), (2L -> 4L), (3L -> 4L)), defaultParallelism)
    val graph = Graph.fromEdgeTuples(edges, true).partitionBy(PartitionStrategy.RandomVertexCut).cache()
    val (testGraph, testGCC) = ClusteringCoefficient.run(graph)
    val testVertices = testGraph.vertices.collect()
    val testEdges = testGraph.edges.collect()

    testEdges.length shouldBe 6
    testVertices.length shouldBe 4

    Math.abs(testVertices(0)._2 - 1.0d) should be < floatingPointEqualityThreshold
    Math.abs(testVertices(1)._2 - 1.0d) should be < floatingPointEqualityThreshold
    Math.abs(testVertices(2)._2 - 1.0d) should be < floatingPointEqualityThreshold
    Math.abs(testVertices(3)._2 - 1.0d) should be < floatingPointEqualityThreshold

    Math.abs(testGCC - 1.0d) should be < floatingPointEqualityThreshold
  }

  "clustering coefficient" should "give correct answer on a simple four node graph " in {

    /*
     * The vertex set of the graph is 1, 2, 3, 4
     * The edge set of the graph is 12, 13, 14, 34
     * A simple inspection reveals that the local clustering coefficients are:
     * vertex 1 has CC 0.333333333333333d
     * vertex 2 has CC 0.0d
     * vertex 3 has CC 1.0d
     * vertex 4 has CC 1.0d
     */
    val edges = sparkContext.parallelize(Array[(Long, Long)]((1L -> 2L), (1L -> 3L), (1L -> 4L), (3L -> 4L)), defaultParallelism)
    val graph = Graph.fromEdgeTuples(edges, true).partitionBy(PartitionStrategy.RandomVertexCut).cache()
    val (testGraph, testGCC) = ClusteringCoefficient.run(graph)
    val testVertices = testGraph.vertices.collect()
    val testEdges = testGraph.edges.collect()

    testEdges.length shouldBe 4
    testVertices.length shouldBe 4

    val testVertexMap = testVertices.toMap

    Math.abs(testVertexMap(1) - 0.33333333333d) should be < floatingPointEqualityThreshold
    Math.abs(testVertexMap(2) - 0.0d) should be < floatingPointEqualityThreshold
    Math.abs(testVertexMap(3) - 1.0d) should be < floatingPointEqualityThreshold
    Math.abs(testVertexMap(4) - 1.0d) should be < floatingPointEqualityThreshold

    Math.abs(testGCC - 0.6d) should be < floatingPointEqualityThreshold
  }
}
