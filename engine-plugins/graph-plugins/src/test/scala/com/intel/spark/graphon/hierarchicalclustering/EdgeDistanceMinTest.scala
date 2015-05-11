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

import org.scalatest.{ Matchers, FlatSpec }

class EdgeDistanceMinTest extends FlatSpec with Matchers {

  val nullEdgeList = null

  val emptyEdgeList = List()

  val basicEdgeList: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 1, 2, 1, 1.1f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 1.2f, false)
  )

  val reversedEdgeList: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 1, 2, 1, 1.1f, false),
    HierarchicalClusteringEdge(2, 1, 1, 1, 1.1f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 1.2f, false)
  )

  "edgeDistance::min" should "return null for null inputs" in {
    val (minEdge, list) = EdgeDistance.min(nullEdgeList)
    assert(minEdge == null)
    assert(list == VertexOutEdges(null, null))
  }

  "edgeDistance::min" should "return null for empty lists" in {
    val (minEdge, list) = EdgeDistance.min(emptyEdgeList)
    assert(minEdge == null)
    assert(list == VertexOutEdges(null, null))
  }

  "edgeDistance::min" should "return non null edge" in {
    val (minEdge, list) = EdgeDistance.min(basicEdgeList)
    assert(minEdge.src == 1)
    assert(minEdge.dest == 2)
  }

  "edgeDistance::min" should "return non null edges" in {
    val (minEdge, list) = EdgeDistance.min(reversedEdgeList)
    assert(minEdge.src == 1)
    assert(minEdge.dest == 2)
  }
}
