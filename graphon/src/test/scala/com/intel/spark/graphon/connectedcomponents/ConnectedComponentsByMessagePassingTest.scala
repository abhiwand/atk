package com.intel.spark.graphon.connectedcomponents

import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.rdd.RDD


class ConnectedComponentsByMessagePassingTest extends FlatSpec with Matchers  with TestingSparkContext {

  trait ConnectedComponentsTest {
    val vertexList: List[Long] = List(1, 2, 3, 5, 6, 7)

    // to save ourselves some pain we enter the directed edge list and then make it bidirectional with a flatmap

    val edgeList: List[(Long, Long)] = List((2.toLong, 6.toLong), (2.toLong,4.toLong), (4.toLong,6.toLong),
      (3.toLong, 5.toLong), (3.toLong, 7.toLong), (5.toLong,7.toLong)).flatMap( {case (x,y) => Set((x,y), (y,x))})


  }

  "connected components" should
    "allocate component IDs according to connectivity equivalence classes" in new ConnectedComponentsTest {

      val rddOfComponentLabeledVertices : RDD[(Long,Long)] =
        ConnectedComponentsByMessagePassing.run(sc.parallelize(vertexList), sc.parallelize(edgeList))

      val vertexIdToComponentMap = rddOfComponentLabeledVertices.collect().toMap

    vertexIdToComponentMap(2) shouldEqual  vertexIdToComponentMap(6)
    vertexIdToComponentMap(2) shouldEqual  vertexIdToComponentMap(4)

    vertexIdToComponentMap(7) shouldEqual  vertexIdToComponentMap(5)
    vertexIdToComponentMap(7) shouldEqual  vertexIdToComponentMap(3)

    vertexIdToComponentMap(2) should not be vertexIdToComponentMap(3)
    vertexIdToComponentMap(3) should not be vertexIdToComponentMap(1)

  }
}
