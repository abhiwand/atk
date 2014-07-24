//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.spark.graphon

import com.intel.graphbuilder.elements.{ Edge, Property, Vertex }
import org.scalatest.Matchers
import com.intel.testutils.TestingSparkContextWordSpec

class GraphStatisticsSpec extends TestingSparkContextWordSpec with Matchers {

  private val validEdgeLabel = "worksWith"
  private val invalidEdgeLabel = "LIKES"

  "Out degrees of vertices in test graph with one edge" in {
    val (vertexSeq, edgeSeq) = createVertexWithOneEdge
    val vertexRDD = sparkContext.parallelize(vertexSeq.toSeq)
    val edgeRDD = sparkContext.parallelize(edgeSeq.toSeq)
    GraphStatistics.outDegrees(edgeRDD).collect().length shouldBe 1
  }

  "In degrees of vertices in test graph" in {
    val (vertexSeq, edgeSeq) = createVertexWithOneEdge
    val vertexRDD = sparkContext.parallelize(vertexSeq.toSeq)
    val edgeRDD = sparkContext.parallelize(edgeSeq.toSeq)
    GraphStatistics.inDegrees(edgeRDD).collect().length shouldBe 1
  }

  "Out degrees of vertices with \"LIKES\" edge-type in test graph" in {
    val (vertexSeq, edgeSeq) = createVertexWithOneEdge
    val vertexRDD = sparkContext.parallelize(vertexSeq.toSeq)
    val edgeRDD = sparkContext.parallelize(edgeSeq.toSeq)
    GraphStatistics.outDegreesByEdgeType(edgeRDD, invalidEdgeLabel).collect().length shouldBe 0
  }

  "In degrees of vertices with \"LIKES\" edge-type in test graph" in {
    val (vertexSeq, edgeSeq) = createVertexWithOneEdge
    val vertexRDD = sparkContext.parallelize(vertexSeq.toSeq)
    val edgeRDD = sparkContext.parallelize(edgeSeq.toSeq)
    GraphStatistics.inDegreesByEdgeType(edgeRDD, invalidEdgeLabel).collect().length shouldBe 0
  }

  "Out degrees of vertices with \"lives\" edge-type in test graph" in {
    val (vertexSeq, edgeSeq) = createVertexWithOneEdge
    val vertexRDD = sparkContext.parallelize(vertexSeq.toSeq)
    val edgeRDD = sparkContext.parallelize(edgeSeq.toSeq)
    GraphStatistics.outDegreesByEdgeType(edgeRDD, validEdgeLabel).collect().length shouldBe 1
  }

  "In degrees of vertices with \"lives\" edge-type in test graph" in {
    val (vertexSeq, edgeSeq) = createVertexWithOneEdge
    val vertexRDD = sparkContext.parallelize(vertexSeq.toSeq)
    val edgeRDD = sparkContext.parallelize(edgeSeq.toSeq)
    GraphStatistics.inDegreesByEdgeType(edgeRDD, validEdgeLabel).collect().length shouldBe 1
  }

  private def createVertexWithOneEdge: (Seq[Vertex], Seq[Edge]) = {
    val alice_gbId = new Property("gbId", 10001)
    val alice = new Vertex(alice_gbId, List(new Property("Name", "Alice")))

    val bob_gbId = new Property("gbId", 10002)
    val bob = new Vertex(bob_gbId, List(new Property("Name", "Bob")))

    val edge1 = new Edge(alice_gbId, bob_gbId, validEdgeLabel, List(new Property("time", 20)))
    (Seq(alice, bob), Seq(edge1))
  }
}
