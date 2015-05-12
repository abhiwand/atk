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

package com.intel.spark.graphon.idassigner

import org.scalatest.{ FlatSpec, Matchers }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.intel.testutils.TestingSparkContextFlatSpec

class GraphIDAssignerTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait GraphIDAssignerTest {
    val one: String = "1"
    val two: String = "2"
    val three: String = "3"
    val four: String = "4"
    val five: String = "5"
    val six: String = "6"
    val seven: String = "7"

    val vertexList: List[String] = List(one, two, three, four, five, six, seven)

    // to save ourselves some pain we enter the directed edge list and then make it bidirectional with a flatmap

    val edgeList: List[(String, String)] = List((two, six), (two, four), (four, six),
      (three, five), (three, seven), (five, seven)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val idAssigner = new GraphIDAssigner[String]()
  }

  "ID assigner" should
    "produce distinct IDs" in new GraphIDAssignerTest {

      val out = idAssigner.run(sparkContext.parallelize(vertexList, 3), sparkContext.parallelize(edgeList, 3))

      out.vertices.distinct().count() shouldEqual out.vertices.count()
    }

  "ID assigner" should
    "produce one  ID for each incoming vertex" in new GraphIDAssignerTest {

      val out = idAssigner.run(sparkContext.parallelize(vertexList, 3), sparkContext.parallelize(edgeList, 3))

      out.vertices.count() shouldEqual vertexList.size
    }

  "ID assigner" should
    "produce the same number of edges in the renamed graph as in the input graph" in new GraphIDAssignerTest {

      val out = idAssigner.run(sparkContext.parallelize(vertexList, 3), sparkContext.parallelize(edgeList, 3))

      out.edges.count() shouldEqual edgeList.size
    }

  "ID assigner" should
    "have every edge in the renamed graph correspond to an edge in the old graph under the provided inverse renaming " in
    new GraphIDAssignerTest {

      val out = idAssigner.run(sparkContext.parallelize(vertexList, 3), sparkContext.parallelize(edgeList, 3))

      val renamedEdges: Array[(Long, Long)] = out.edges.collect()

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
