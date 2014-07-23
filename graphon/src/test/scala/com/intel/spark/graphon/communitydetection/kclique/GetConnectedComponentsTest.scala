
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

import org.scalatest.{ Matchers, FlatSpec, FunSuite }
import com.intel.spark.graphon.connectedcomponents.TestingSparkContext
import com.intel.spark.graphon.communitydetection.kclique.{ GraphGenerator, GetConnectedComponents, DataTypes }
import DataTypes._

class GetConnectedComponentsTest extends FlatSpec with Matchers with TestingSparkContext {

  trait getConnectedComponentsTest {
    val fourCliques = List((Array(2, 3, 4), Array(5, 7, 8)), (Array(3, 5, 6), Array(7, 8)))
      .map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })

    val vertexListOfFourCliqueGraph = List(Array(2, 3, 4, 5), Array(2, 3, 4, 7), Array(2, 3, 4, 8), Array(3, 5, 6, 7), Array(3, 5, 6, 8))
      .map(clique => clique.map(_.toLong).toSet)
  }

  "K-Clique Connected Components" should
    "produce the same number of pairs of vertices and component ID as the number of vertices in the input graph" in new getConnectedComponentsTest {

      val rddOfFourCliques = sc.parallelize(fourCliques).map({ case (x, y) => ExtendersFact(CliqueFact(x), y, true) })

      val fourCliqueGraphFromCreateGraphOutput = GraphGenerator.run(rddOfFourCliques)
      val fourCliqueGraphCCOutput = GetConnectedComponents.run(fourCliqueGraphFromCreateGraphOutput, sc)
      val fourCliqueGraphCC = fourCliqueGraphCCOutput.connectedComponents

      fourCliqueGraphCC.count() shouldEqual vertexListOfFourCliqueGraph.size
    }
}
