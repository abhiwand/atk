
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

package com.intel.spark.graphon.communitydetection.kclique

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Property, Vertex }
import com.intel.spark.graphon.communitydetection.ScalaToJavaCollectionConverter

class GBVertexRDDBuilderTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

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

      val rddOfGbVerticesList: RDD[Vertex] = sparkContext.parallelize(gbVerticesList)
      val rddOfVertexCommunitySet: RDD[(Long, Set[Long])] = sparkContext.parallelize(vertexCommunitySet)

      val gbVertexSetter: GBVertexRDDBuilder = new GBVertexRDDBuilder(rddOfGbVerticesList, rddOfVertexCommunitySet)
      val newGBVerticesAsGBVertexSetterOutput: RDD[Vertex] = gbVertexSetter.setVertex(communityPropertyDefaultLabel)

      newGBVerticesAsGBVertexSetterOutput.count() shouldEqual rddOfGbVerticesList.count()

    }

  "The GB vertex list" should
    "include empty community property, if the vertex doesn't belong to any community" in new GBVertexSetTest {

      val rddOfGbVerticesList: RDD[Vertex] = sparkContext.parallelize(gbVerticesList)
      val rddOfVertexCommunitySet: RDD[(Long, Set[Long])] = sparkContext.parallelize(vertexCommunitySet)

      val gbVertexSetter: GBVertexRDDBuilder = new GBVertexRDDBuilder(rddOfGbVerticesList, rddOfVertexCommunitySet)
      val newGBVerticesAsGBVertexSetterOutput: RDD[Vertex] = gbVertexSetter.setVertex(communityPropertyDefaultLabel)

      val rddOfNewGBVertexList: RDD[Vertex] = sparkContext.parallelize(newGBVertexList)

      rddOfNewGBVertexList.collect().toSet shouldEqual newGBVerticesAsGBVertexSetterOutput.collect().toSet

    }

}
