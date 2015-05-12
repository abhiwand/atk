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

package com.intel.spark.graphon.connectedcomponents

import org.scalatest.{ FlatSpec, Matchers }
import org.apache.spark.rdd.RDD
import com.intel.testutils.TestingSparkContextFlatSpec

class ConnectedComponentsPluginGraphXDefaultTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait ConnectedComponentsTest {
    val vertexList: List[Long] = List(1, 2, 3, 5, 6, 7)

    // to save ourselves some pain we enter the directed edge list and then make it bidirectional with a flatmap

    val edgeList: List[(Long, Long)] = List((2.toLong, 6.toLong), (2.toLong, 4.toLong), (4.toLong, 6.toLong),
      (3.toLong, 5.toLong), (3.toLong, 7.toLong), (5.toLong, 7.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

  }

  "connected components" should
    "allocate component IDs according to connectivity equivalence classes" in new ConnectedComponentsTest {

      val rddOfComponentLabeledVertices: RDD[(Long, Long)] =
        ConnectedComponentsGraphXDefault.run(sparkContext.parallelize(vertexList), sparkContext.parallelize(edgeList))

      val vertexIdToComponentMap = rddOfComponentLabeledVertices.collect().toMap

      vertexIdToComponentMap(2) shouldEqual vertexIdToComponentMap(6)
      vertexIdToComponentMap(2) shouldEqual vertexIdToComponentMap(4)

      vertexIdToComponentMap(7) shouldEqual vertexIdToComponentMap(5)
      vertexIdToComponentMap(7) shouldEqual vertexIdToComponentMap(3)

      vertexIdToComponentMap(2) should not be vertexIdToComponentMap(3)
      vertexIdToComponentMap(3) should not be vertexIdToComponentMap(1)

    }
}
