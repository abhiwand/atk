////////////////////////////////////////////////////////////////////////////////
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
////////////////////////////////////////////////////////////////////////////////

package com.intel.spark.graphon.sampling

import com.intel.testutils.TestingSparkContextWordSpec
import com.intel.spark.graphon.testutils.TestingTitan
import com.intel.graphbuilder.elements.{ Property, Vertex, Edge }
import scala.collection.JavaConversions._
import org.scalatest.Matchers

/**
 * Integration testing for uniform vertex sampling
 */
class VertexSampleITest extends TestingSparkContextWordSpec with Matchers {

  // generate sample data
  val gbIds = Map((1, new Property("gbId", 1)),
    (2, new Property("gbId", 2)),
    (3, new Property("gbId", 3)),
    (4, new Property("gbId", 4)),
    (5, new Property("gbId", 5)),
    (6, new Property("gbId", 6)),
    (7, new Property("gbId", 7)),
    (8, new Property("gbId", 8)))

  val inputVertexList = Seq(Vertex(gbIds(1), gbIds(1), Seq(new Property("location", "Oregon"))),
    Vertex(gbIds(2), gbIds(2), Seq(new Property("location", "Oregon"))),
    Vertex(gbIds(3), gbIds(3), Seq(new Property("location", "Oregon"))),
    Vertex(gbIds(4), gbIds(4), Seq(new Property("location", "Oregon"))),
    Vertex(gbIds(5), gbIds(5), Seq(new Property("location", "Oregon"))),
    Vertex(gbIds(6), gbIds(6), Seq(new Property("location", "Oregon"))),
    Vertex(gbIds(7), gbIds(7), Seq(new Property("location", "Oregon"))),
    Vertex(gbIds(8), gbIds(8), Seq(new Property("location", "Oregon"))))

  val inputVertexListWeighted = Seq((0.5, Vertex(gbIds(1), gbIds(1), Seq(new Property("location", "Oregon")))),
    (0.1, Vertex(gbIds(2), gbIds(2), Seq(new Property("location", "Oregon")))),
    (2.0, Vertex(gbIds(3), gbIds(3), Seq(new Property("location", "Oregon")))),
    (1.1, Vertex(gbIds(4), gbIds(4), Seq(new Property("location", "Oregon")))),
    (0.3, Vertex(gbIds(5), gbIds(5), Seq(new Property("location", "Oregon")))),
    (3.6, Vertex(gbIds(6), gbIds(6), Seq(new Property("location", "Oregon")))),
    (1.5, Vertex(gbIds(7), gbIds(7), Seq(new Property("location", "Oregon")))),
    (1.4, Vertex(gbIds(8), gbIds(8), Seq(new Property("location", "Oregon")))))

  val inputEdgeList = Seq(Edge(gbIds(1), gbIds(2), gbIds(1), gbIds(2), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(1), gbIds(3), gbIds(1), gbIds(3), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(1), gbIds(4), gbIds(1), gbIds(4), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(2), gbIds(1), gbIds(2), gbIds(1), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(2), gbIds(5), gbIds(2), gbIds(5), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(3), gbIds(1), gbIds(3), gbIds(1), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(3), gbIds(4), gbIds(3), gbIds(4), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(3), gbIds(6), gbIds(3), gbIds(6), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(3), gbIds(7), gbIds(3), gbIds(7), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(3), gbIds(8), gbIds(3), gbIds(8), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(4), gbIds(1), gbIds(4), gbIds(1), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(4), gbIds(3), gbIds(4), gbIds(3), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(5), gbIds(2), gbIds(5), gbIds(2), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(5), gbIds(6), gbIds(5), gbIds(6), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(5), gbIds(7), gbIds(5), gbIds(7), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(6), gbIds(3), gbIds(6), gbIds(3), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(6), gbIds(5), gbIds(6), gbIds(5), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(7), gbIds(3), gbIds(7), gbIds(3), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(7), gbIds(5), gbIds(7), gbIds(5), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
    Edge(gbIds(8), gbIds(3), gbIds(8), gbIds(3), "tweeted", Seq(new Property("tweet", "blah blah blah..."))))

  "Generating a uniform vertex sample" should {

    "contain correct number of vertices in sample" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)

      val sampleVerticesRdd = VertexSampleSparkOps.sampleVerticesUniform(vertexRdd, 5, None)
      sampleVerticesRdd.count() shouldEqual 5
    }

    "give error if sample size less than 1" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)

      intercept[IllegalArgumentException] {
        VertexSampleSparkOps.sampleVerticesUniform(vertexRdd, 0, None)
      }
    }

    "returns entire dataset if sample size is greater than or equal to dataset size" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)

      VertexSampleSparkOps.sampleVerticesUniform(vertexRdd, 200, None) shouldEqual vertexRdd
    }
  }

  "Generating a degree-weighted vertex sample" should {

    "contain correct number of vertices in sample" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val sampleVerticesRdd = VertexSampleSparkOps.sampleVerticesDegree(vertexRdd, edgeRdd, 5, None)
      sampleVerticesRdd.count() shouldEqual 5
    }

    "give error if sample size less than 1" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      intercept[IllegalArgumentException] {
        VertexSampleSparkOps.sampleVerticesDegree(vertexRdd, edgeRdd, 0, None)
      }
    }

    "returns entire dataset if sample size is greater than or equal to dataset size" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      VertexSampleSparkOps.sampleVerticesDegree(vertexRdd, edgeRdd, 200, None) shouldEqual vertexRdd
    }
  }

  "Generating a degree distribution-weighted vertex sample" should {

    "contain correct number of vertices in sample" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val sampleVerticesRdd = VertexSampleSparkOps.sampleVerticesDegreeDist(vertexRdd, edgeRdd, 5, None)
      sampleVerticesRdd.count() shouldEqual 5
    }

    "give error if sample size less than 1" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      intercept[IllegalArgumentException] {
        VertexSampleSparkOps.sampleVerticesDegreeDist(vertexRdd, edgeRdd, 0, None)
      }
    }

    "returns entire dataset if sample size is greater than or equal to dataset size" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      VertexSampleSparkOps.sampleVerticesDegreeDist(vertexRdd, edgeRdd, 200, None) shouldEqual vertexRdd
    }
  }

  "Generating a vertex sample" should {

    "generate correct vertex induced subgraph" in {

      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val sampleVertexList = Seq(Vertex(gbIds(1), gbIds(1), Seq(new Property("location", "Oregon"))),
        Vertex(gbIds(2), gbIds(2), Seq(new Property("location", "Oregon"))),
        Vertex(gbIds(3), gbIds(3), Seq(new Property("location", "Oregon"))),
        Vertex(gbIds(4), gbIds(4), Seq(new Property("location", "Oregon"))))

      val sampleEdgeList = Seq(Edge(gbIds(1), gbIds(2), gbIds(1), gbIds(2), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
        Edge(gbIds(1), gbIds(3), gbIds(1), gbIds(3), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
        Edge(gbIds(1), gbIds(4), gbIds(1), gbIds(4), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
        Edge(gbIds(2), gbIds(1), gbIds(2), gbIds(1), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
        Edge(gbIds(3), gbIds(1), gbIds(3), gbIds(1), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
        Edge(gbIds(3), gbIds(4), gbIds(3), gbIds(4), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
        Edge(gbIds(4), gbIds(1), gbIds(4), gbIds(1), "tweeted", Seq(new Property("tweet", "blah blah blah..."))),
        Edge(gbIds(4), gbIds(3), gbIds(4), gbIds(3), "tweeted", Seq(new Property("tweet", "blah blah blah..."))))

      val sampleVertexRdd = sparkContext.parallelize(sampleVertexList, 2)
      val sampleEdgeRdd = sparkContext.parallelize(sampleEdgeList, 2)

      val subgraphEdges = VertexSampleSparkOps.vertexInducedEdgeSet(sampleVertexRdd, edgeRdd)

      subgraphEdges.count() shouldEqual sampleEdgeRdd.count()
      subgraphEdges.subtract(sampleEdgeRdd).count() shouldEqual 0
    }

    "correctly write the vertex induced subgraph to Titan" in new TestingTitan {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      VertexSampleSparkOps.writeToTitan(vertexRdd, edgeRdd, titanConfig)

      graph = titanConnector.connect()

      graph.getEdges.size shouldEqual 20
      graph.getVertices.size shouldEqual 8
    }

    "select the correct weighted vertices" in {
      val vertexRdd = sparkContext.parallelize(inputVertexListWeighted, 2)

      val topVertexRdd = VertexSampleSparkOps.getTopVertices(vertexRdd, 4)
      val topVertexArray = topVertexRdd.collect()

      topVertexArray.contains(inputVertexList(5)) shouldEqual true
      topVertexArray.contains(inputVertexList(2)) shouldEqual true
      topVertexArray.contains(inputVertexList(6)) shouldEqual true
      topVertexArray.contains(inputVertexList(7)) shouldEqual true
    }
  }

  "Degree weighted sampling" should {

    "add correct vertex weights" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val weightedVertexRdd = VertexSampleSparkOps.addVertexDegreeWeights(vertexRdd, edgeRdd)
      val weightedVertexArray = weightedVertexRdd.take(8).map { case (weight, vertex) => (vertex, weight) }.toMap

      weightedVertexArray(inputVertexList(0)) shouldEqual 3l
      weightedVertexArray(inputVertexList(1)) shouldEqual 2l
      weightedVertexArray(inputVertexList(2)) shouldEqual 5l
      weightedVertexArray(inputVertexList(3)) shouldEqual 2l
      weightedVertexArray(inputVertexList(4)) shouldEqual 3l
      weightedVertexArray(inputVertexList(5)) shouldEqual 2l
      weightedVertexArray(inputVertexList(6)) shouldEqual 2l
      weightedVertexArray(inputVertexList(7)) shouldEqual 1l
    }
  }

  "DegreeDist weighted sampling" should {

    "add correct vertex weights" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val weightedVertexRdd = VertexSampleSparkOps.addVertexDegreeDistWeights(vertexRdd, edgeRdd)
      val weightedVertexArray = weightedVertexRdd.take(8).map { case (weight, vertex) => (vertex, weight) }.toMap

      weightedVertexArray(inputVertexList(0)) shouldEqual 2l
      weightedVertexArray(inputVertexList(1)) shouldEqual 4l
      weightedVertexArray(inputVertexList(2)) shouldEqual 1l
      weightedVertexArray(inputVertexList(3)) shouldEqual 4l
      weightedVertexArray(inputVertexList(4)) shouldEqual 2l
      weightedVertexArray(inputVertexList(5)) shouldEqual 4l
      weightedVertexArray(inputVertexList(6)) shouldEqual 4l
      weightedVertexArray(inputVertexList(7)) shouldEqual 1l
    }
  }

}
