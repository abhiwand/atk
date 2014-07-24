
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
import com.intel.spark.graphon.connectedcomponents.TestingSparkContext
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.kclique.datatypes.{ ExtendersFact, CliqueFact, Edge }

class CliqueEnumeratorTest extends FlatSpec with Matchers with TestingSparkContext {

  trait KCliqueEnumTest {

    val vertexWithAdjacencyList: List[(Long, Array[Long])] = List((1, Array(2, 3, 4)), (2, Array(3, 4)), (3, Array(4, 5))).map(
      { case (v, nbrs) => (v.toLong, nbrs.map(_.toLong)) })

    val edgeList: List[(Long, Long)] = List((1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4), (3, 5)).map({ case (x, y) => (x.toLong, y.toLong) })

    val fourCliques = List((Array(1, 2, 3), Array(4))).map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })

  }

  "K-Clique enumeration" should
    "create all set of k-cliques" in new KCliqueEnumTest {

      val rddOfEdgeList: RDD[Edge] = sc.parallelize(edgeList).map(keyval => Edge(keyval._1, keyval._2))
      val rddOfFourCliques = sc.parallelize(fourCliques).map({ case (x, y) => ExtendersFact(CliqueFact(x), y, true) })

      val enumeratedFourCliques = CliqueEnumerator.run(rddOfEdgeList, 4)

      enumeratedFourCliques.collect().toSet shouldEqual rddOfFourCliques.collect().toSet

    }

}