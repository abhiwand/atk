package com.intel.spark.graphon.communitydetection.kclique

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.spark.graphon.connectedcomponents.TestingSparkContext
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Property, Vertex }
import com.intel.spark.graphon.communitydetection.ScalaToJavaCollectionConverter

class GBVertexSetterTest extends FlatSpec with Matchers with TestingSparkContext {

  trait GBVertexSetTest {

    val communityPropertyDefaultLabel: String = "Community"

    val gbVerticesList: List[Vertex] = List(
      new Vertex(java.lang.Long.valueOf(10001), new Property("titanPhysicalId", 10001), List(new Property("source", 101))),
      new Vertex(java.lang.Long.valueOf(10002), new Property("titanPhysicalId", 10002), List(new Property("source", 102))),
      new Vertex(java.lang.Long.valueOf(10003), new Property("titanPhysicalId", 10003), List(new Property("source", 103))),
      new Vertex(java.lang.Long.valueOf(10004), new Property("titanPhysicalId", 10004), List(new Property("source", 104))))

    val vertexCommunitySet: List[(Long, Set[Long])] = List(
      (10001, Array(1)),
      (10003, Array(1, 2)),
      (10004, Array(1))
    ).map({ case (vertex, communityArray) => (vertex.toLong, communityArray.map(_.toLong).toSet) })

    val emptySet: Set[Long] = Set()
    val newGBVertexList = List(
      new Vertex(java.lang.Long.valueOf(10001), new Property("titanPhysicalId", 10001),
        List(new Property(communityPropertyDefaultLabel, ScalaToJavaCollectionConverter.convertSet(Set(1.toLong))))),
      new Vertex(java.lang.Long.valueOf(10002), new Property("titanPhysicalId", 10002),
        List(new Property(communityPropertyDefaultLabel, ScalaToJavaCollectionConverter.convertSet(emptySet)))),
      new Vertex(java.lang.Long.valueOf(10003), new Property("titanPhysicalId", 10003),
        List(new Property(communityPropertyDefaultLabel, ScalaToJavaCollectionConverter.convertSet(Set(1.toLong, 2.toLong))))),
      new Vertex(java.lang.Long.valueOf(10004), new Property("titanPhysicalId", 10004),
        List(new Property(communityPropertyDefaultLabel, ScalaToJavaCollectionConverter.convertSet(Set(1.toLong))))))

  }

  "Number of vertices coming out" should
    "be same with original input graph" in new GBVertexSetTest {

      val rddOfGbVerticesList: RDD[Vertex] = sc.parallelize(gbVerticesList)
      val rddOfVertexCommunitySet: RDD[(Long, Set[Long])] = sc.parallelize(vertexCommunitySet)

      val gbVertexSetter: GBVertexSetter = new GBVertexSetter(rddOfGbVerticesList, rddOfVertexCommunitySet)
      val newGBVerticesAsGBVertexSetterOutput: RDD[Vertex] = gbVertexSetter.setVertex(communityPropertyDefaultLabel)

      newGBVerticesAsGBVertexSetterOutput.count() shouldEqual rddOfGbVerticesList.count()

    }

  "The GB vertex list" should
    "include empty community property, if the vertex doesn't belong to any community" in new GBVertexSetTest {

      val rddOfGbVerticesList: RDD[Vertex] = sc.parallelize(gbVerticesList)
      val rddOfVertexCommunitySet: RDD[(Long, Set[Long])] = sc.parallelize(vertexCommunitySet)

      val gbVertexSetter: GBVertexSetter = new GBVertexSetter(rddOfGbVerticesList, rddOfVertexCommunitySet)
      val newGBVerticesAsGBVertexSetterOutput: RDD[Vertex] = gbVertexSetter.setVertex(communityPropertyDefaultLabel)

      val rddOfNewGBVertexList: RDD[Vertex] = sc.parallelize(newGBVertexList)

      rddOfNewGBVertexList.collect().toSet shouldEqual newGBVerticesAsGBVertexSetterOutput.collect().toSet

    }

}
