package com.intel.spark.graphon.idassigner

import com.intel.spark.graphon.connectedcomponents.{ TestingSparkContext }
import org.scalatest.{ FlatSpec, Matchers }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class GraphIDAssignerTest extends FlatSpec with Matchers with TestingSparkContext {

  trait GraphIDAssignerTest {
    val vertexList: List[Long] = List(1, 2, 3, 4, 5, 6, 7)

    // to save ourselves some pain we enter the directed edge list and then make it bidirectional with a flatmap

    val edgeList: List[(Long, Long)] = List((2.toLong, 6.toLong), (2.toLong, 4.toLong), (4.toLong, 6.toLong),
      (3.toLong, 5.toLong), (3.toLong, 7.toLong), (5.toLong, 7.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val idAssigner = new GraphIDAssigner[Long](sc)
  }

  "ID assigner" should
    "produce distinct IDs" in new GraphIDAssignerTest {

      val out = idAssigner.run(sc.parallelize(vertexList), sc.parallelize(edgeList))

      out.vertices.distinct().count() shouldEqual out.vertices.count()
    }

  "ID assigner" should
    "produce one  ID for each incoming vertex" in new GraphIDAssignerTest {

      val out = idAssigner.run(sc.parallelize(vertexList), sc.parallelize(edgeList))

      out.vertices.count() shouldEqual vertexList.size
    }

  "ID assigner" should
    "produce the same number of edges in the renamed graph as in the input graph" in new GraphIDAssignerTest {

      val out = idAssigner.run(sc.parallelize(vertexList), sc.parallelize(edgeList))

      out.edges.count() shouldEqual edgeList.size
    }

  "ID assigner" should
    "have every edge in the renamed graph correspond to an edge in the old graph under the provided inverse renaming " in
    new GraphIDAssignerTest {

      val out = idAssigner.run(sc.parallelize(vertexList), sc.parallelize(edgeList))

      val renamedEdges: Array[(Long, Long)] = out.edges.toArray()

      def renamed(srcNewId: Long, dstNewId: Long) = {
        (out.newIdsToOld.lookup(srcNewId), out.newIdsToOld.lookup(dstNewId))
      }

      renamedEdges.forall({ case (srcNewId, dstNewId) => edgeList.contains(renamed(srcNewId, dstNewId)) })
    }

  /**
   * We do not need to check that every edge in the base graph can be found in the renamed graph because we have checked
   * that the count of edges is the same in both graphs,  we know that every renamed edge is in the base graph
   * (after we undo the renaming), and we know that the renaming is an injection from the vertex set.
   */
}

