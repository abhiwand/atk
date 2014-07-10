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

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import org.apache.spark.rdd.RDD
import org.specs2.mutable.Specification
import com.intel.testutils.TestingSparkContext
import com.intel.spark.graphon.testutils.TestingTitan
import com.intel.graphbuilder.elements.{ Property, Vertex, Edge }
import scala.collection.JavaConversions._

/**
 * Integration testing for uniform vertex sampling
 */
class VertexSampleITest extends Specification {

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

    "contain correct number of vertices in sample" in new TestingSparkContext {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)

      val vs = new VertexSample
      val sampleVerticesRdd = vs.sampleVerticesUniform(vertexRdd, 5)
      sampleVerticesRdd.count() mustEqual 5
    }

    "give error if sample size less than 1" in new TestingSparkContext {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)

      val vs = new VertexSample
      vs.sampleVerticesUniform(vertexRdd, 0) must throwAn[IllegalArgumentException]
    }

    "returns entire dataset if sample size is greater than or equal to dataset size" in new TestingSparkContext {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)

      val vs = new VertexSample
      vs.sampleVerticesUniform(vertexRdd, 200) mustEqual vertexRdd
    }
  }

  "Generating a degree-weighted vertex sample" should {

    "contain correct number of vertices in sample" in new TestingSparkContext with TestingTitan {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)
      val edgeRdd = sc.parallelize(inputEdgeList.toSeq, 2)

      val vs = new VertexSample
      val sampleVerticesRdd = vs.sampleVerticesDegree(vertexRdd, edgeRdd, 5)
      sampleVerticesRdd.count() mustEqual 5
    }

    "give error if sample size less than 1" in new TestingSparkContext {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)
      val edgeRdd = sc.parallelize(inputEdgeList.toSeq, 2)

      val vs = new VertexSample
      vs.sampleVerticesDegree(vertexRdd, edgeRdd, 0) must throwAn[IllegalArgumentException]
    }

    "returns entire dataset if sample size is greater than or equal to dataset size" in new TestingSparkContext {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)
      val edgeRdd = sc.parallelize(inputEdgeList.toSeq, 2)

      val vs = new VertexSample
      vs.sampleVerticesDegree(vertexRdd, edgeRdd, 200) mustEqual vertexRdd
    }
  }

  "Generating a degree distribution-weighted vertex sample" should {

    "contain correct number of vertices in sample" in new TestingSparkContext {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)
      val edgeRdd = sc.parallelize(inputEdgeList.toSeq, 2)

      val vs = new VertexSample
      val sampleVerticesRdd = vs.sampleVerticesDegreeDist(vertexRdd, edgeRdd, 5)
      sampleVerticesRdd.count() mustEqual 5
    }

    "give error if sample size less than 1" in new TestingSparkContext {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)
      val edgeRdd = sc.parallelize(inputEdgeList.toSeq, 2)

      val vs = new VertexSample
      vs.sampleVerticesDegreeDist(vertexRdd, edgeRdd, 0) must throwAn[IllegalArgumentException]
    }

    "returns entire dataset if sample size is greater than or equal to dataset size" in new TestingSparkContext {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)
      val edgeRdd = sc.parallelize(inputEdgeList.toSeq, 2)

      val vs = new VertexSample
      vs.sampleVerticesDegreeDist(vertexRdd, edgeRdd, 200) mustEqual vertexRdd
    }
  }

  "Generating a vertex sample" should {

    "generate correct vertex induced subgraph" in new TestingSparkContext {

      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)
      val edgeRdd = sc.parallelize(inputEdgeList.toSeq, 2)

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

      val sampleVertexRdd = sc.parallelize(sampleVertexList.toSeq, 2)
      val sampleEdgeRdd = sc.parallelize(sampleEdgeList.toSeq, 2)

      val vs = new VertexSample
      val subgraphEdges = vs.sampleEdges(sampleVertexRdd, edgeRdd)

      subgraphEdges.count() mustEqual sampleEdgeRdd.count()
      subgraphEdges.subtract(sampleEdgeRdd).count() mustEqual 0
    }

    "correctly write the vertex induced subgraph to Titan" in new TestingSparkContext with TestingTitan {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)
      val edgeRdd = sc.parallelize(inputEdgeList.toSeq, 2)

      val vs = new VertexSample
      vs.writeToTitan(vertexRdd, edgeRdd, titanConfig)

      graph = titanConnector.connect()

      graph.getEdges.size mustEqual 20
      graph.getVertices.size mustEqual 8
    }

    /*    "correctly read the input graph from Titan into Vertex and Edge RDDs" in new TestingSparkContext {
      val vertexRdd = sc.parallelize(inputVertexList.toSeq, 2)
      val edgeRdd = sc.parallelize(inputEdgeList.toSeq, 2)

      val titanConfig = new SerializableBaseConfiguration()
      titanConfig.setProperty("storage.backend", "hbase")
      titanConfig.setProperty("storage.hostname", "fairlane")
      titanConfig.setProperty("storage.port", "2181")
      titanConfig.setProperty("storage.tablename", "titanTestGraph")

      val vs = new VertexSample

      // TODO: delete preexisting graph or it will simply append and cause test to fail
      vs.writeToTitan(vertexRdd, edgeRdd, titanConfig)

      titanConfig.clearProperty("storage.tablename")

      val (readVertexRdd: RDD[Vertex], readEdgeRdd: RDD[Edge]) = vs.getGraph("titanTestGraph", sc, titanConfig)

      readEdgeRdd.count() mustEqual 20
      readVertexRdd.count() mustEqual 8
    }*/
  }

}
