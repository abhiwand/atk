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

package com.intel.spark.graphon.hierarchicalclustering

import org.scalatest.{ FlatSpec, Matchers }

class EdgeDistanceAvgTest extends FlatSpec with Matchers {

  val emptyEdgeList = List()

  val basicEdgeList: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 1, 2, 1, 1.1f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 1.3f, false)
  )

  val reversedEdgeList: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 4, 2, 1, 1.0f, false),
    HierarchicalClusteringEdge(2, 1, 1, 1, 1.4f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 1.2f, false)
  )

  "edgeDistance::weightedAvg" should "be 0 for empty lists" in {
    val dist = EdgeDistance.weightedAvg(emptyEdgeList)
    assert(dist == 0)
  }

  "edgeDistance::weightedAvg" should "be non 0" in {
    val dist = EdgeDistance.weightedAvg(basicEdgeList)
    assert(dist == 1.2f)
  }

  "edgeDistance::weightedAvg" should "return non 0 value" in {
    val dist = EdgeDistance.weightedAvg(reversedEdgeList)
    assert(dist == 1.1f)
  }

  "edgeDistance::simpleAvg" should "be 0 for empty lists" in {
    val edge = EdgeDistance.simpleAvg(emptyEdgeList, false)
    assert(edge == null)
  }

  "edgeDistance::simpleAvg" should "be non 0" in {
    val edge = EdgeDistance.simpleAvg(basicEdgeList, false)
    assert(edge.distance == 1.2f)
  }

  "edgeDistance::simpleAvg" should "return non 0 value" in {
    val edge = EdgeDistance.simpleAvg(reversedEdgeList, false)
    assert(edge.distance == 1.2f)
  }
}
