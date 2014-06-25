package com.intel.spark.graphon.idassigner

import com.intel.spark.graphon.connectedcomponents.{ConnectedComponentsGraphXDefault, TestingSparkContext}
import org.scalatest.{FlatSpec, Matchers}

import org.apache.spark.rdd.RDD

/**
 * Created by nlsegerl on 6/24/14.
 */
class GraphIDAssignerTest extends FlatSpec with Matchers  with TestingSparkContext {

  trait GraphIDAssignerTest {
    val vertexList: List[Long] = List(1, 2, 3, 5, 6, 7)

    // to save ourselves some pain we enter the directed edge list and then make it bidirectional with a flatmap

    val edgeList: List[(Long, Long)] = List((2.toLong, 6.toLong), (2.toLong,4.toLong), (4.toLong,6.toLong),
      (3.toLong, 5.toLong), (3.toLong, 7.toLong), (5.toLong,7.toLong)).flatMap( {case (x,y) => Set((x,y), (y,x))})

    val idAssigner = new GraphIDAssigner[Long](sc)
  }

  "ID assigner" should
    "produce one distinct IDs for each incoming vertex" in new GraphIDAssignerTest {

    val out = idAssigner.run(sc.parallelize(vertexList), sc.parallelize(edgeList))

    out.vertices.distinct().count() shouldEqual vertexList.size
  }

  "ID assigner" should
    "produce the same number of edges in the renamed graph as in the input graph" in new GraphIDAssignerTest {

    val out = idAssigner.run(sc.parallelize(vertexList), sc.parallelize(edgeList))

    out.edges.count() shouldEqual edgeList.size
  }

}

