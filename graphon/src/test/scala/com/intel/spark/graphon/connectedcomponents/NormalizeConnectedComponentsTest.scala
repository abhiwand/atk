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

import org.scalatest.Matchers

import java.util.Date
import org.apache.spark.SparkContext
import scala.concurrent.Lock
import org.scalatest.{ FlatSpec, BeforeAndAfter }
import org.apache.log4j.{ Logger, Level }
import com.intel.testutils.TestingSparkContextFlatSpec

class NormalizeConnectedComponentsTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait ConnectedComponentsTest {
    val vertexList: List[Long] = List(4, 8, 12, 16, 20, 24, 28, 32)
    val originalEquivalenceClasses: Set[Set[Long]] = Set(Set(8, 24, 32), Set(4, 16, 28), Set(12))
    val originalVertexToComponent: List[(Long, Long)] = List((8.toLong, 8.toLong),
      (24.toLong, 8.toLong), (32.toLong, 8.toLong), (4.toLong, 28.toLong), (16.toLong, 28.toLong),
      (28.toLong, 28.toLong), (12.toLong, 12.toLong))
  }

  "normalize connected components" should "not have any gaps in the range of component IDs returned" in new ConnectedComponentsTest {

    val verticesToComponentsRDD = sparkContext.parallelize(originalVertexToComponent).map(x => (x._1.toLong, x._2.toLong))
    val normalizeOut = NormalizeConnectedComponents.normalize(verticesToComponentsRDD, sparkContext)

    val normalizeOutComponents = normalizeOut._2.map(x => x._2).distinct()

    normalizeOutComponents.map(_.toLong).collect().toSet shouldEqual (1.toLong to (normalizeOutComponents.count().toLong)).toSet
  }

  "normalize connected components" should "create correct equivalence classes" in new ConnectedComponentsTest {

    val verticesToComponentsRDD = sparkContext.parallelize(originalVertexToComponent).map(x => (x._1.toLong, x._2.toLong))
    val normalizedCCROutput = NormalizeConnectedComponents.normalize(verticesToComponentsRDD, sparkContext)

    val vertexComponentPairs = normalizedCCROutput._2.collect()

    val groupedVertexComponentPairs = vertexComponentPairs.groupBy(x => x._2).map(x => x._2)

    val setsOfPairs = groupedVertexComponentPairs.map(list => list.toSet)

    val equivalenceclasses = setsOfPairs.map(set => set.map(x => x._1)).toSet

    equivalenceclasses shouldEqual originalEquivalenceClasses
  }

}
