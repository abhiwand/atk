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

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.{ FlatSpec, Matchers }
import org.apache.spark.rdd.RDD

case class HierarchicalClusteringStorageMock() extends HierarchicalClusteringStorageInterface {
  val mockNodeId = 100
  var newMetaNodes = 0

  def addSchema(): Unit = {
  }

  def addVertexAndEdges(src: Long, dest: Long, metaNodeCount: Long, metaNodeName: String, iteration: Int): Long = {

    newMetaNodes = newMetaNodes + 1
    mockNodeId + newMetaNodes
  }

  def commit(): Unit = {
  }

  def shutdown(): Unit = {
  }
}

case class HierarchicalClusteringStorageFactoryMock(dbConnectionConfig: SerializableBaseConfiguration)
    extends HierarchicalClusteringStorageFactoryInterface {

  override def newStorage(): HierarchicalClusteringStorageInterface = {

    new HierarchicalClusteringStorageMock
  }
}

class HierarchicalClusteringWorkerTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val emptyEdgeList = List()

  val edgeListOneIteration: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 1, 2, 1, 0.9f, false),
    HierarchicalClusteringEdge(2, 1, 1, 1, 0.9f, false)
  )

  val edgeListTwoIterations: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 1, 2, 1, 0.9f, false),
    HierarchicalClusteringEdge(2, 1, 1, 1, 0.9f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 0.8f, false),
    HierarchicalClusteringEdge(3, 1, 2, 1, 0.8f, false)
  )

  val edgeListThreeIterations: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 1, 2, 1, 0.1f, false),
    HierarchicalClusteringEdge(2, 1, 1, 1, 0.1f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 0.8f, false),
    HierarchicalClusteringEdge(3, 1, 2, 1, 0.8f, false),
    HierarchicalClusteringEdge(3, 1, 4, 1, 0.7f, false),
    HierarchicalClusteringEdge(4, 1, 3, 1, 0.7f, false),
    HierarchicalClusteringEdge(4, 1, 5, 1, 0.1f, false),
    HierarchicalClusteringEdge(5, 1, 4, 1, 0.1f, false)
  )

  val edgeListFourIterations: List[HierarchicalClusteringEdge] = List(
    HierarchicalClusteringEdge(1, 1, 2, 1, 0.9f, false),
    HierarchicalClusteringEdge(2, 1, 1, 1, 0.9f, false),
    HierarchicalClusteringEdge(2, 1, 3, 1, 0.8f, false),
    HierarchicalClusteringEdge(3, 1, 2, 1, 0.8f, false),
    HierarchicalClusteringEdge(3, 1, 4, 1, 0.7f, false),
    HierarchicalClusteringEdge(4, 1, 3, 1, 0.7f, false),
    HierarchicalClusteringEdge(4, 1, 5, 1, 0.9f, false),
    HierarchicalClusteringEdge(5, 1, 4, 1, 0.9f, false)
  )

  def executeTest(edgeList: List[HierarchicalClusteringEdge], iterationsToComplete: Int): Unit = {
    val worker = new HierarchicalClusteringWorker(null)
    val hcFactoryMock = new HierarchicalClusteringStorageFactoryMock(null)

    val report = worker.clusterGraph(sparkContext.parallelize(edgeList), hcFactoryMock)
    val iterations = HierarchicalClusteringConstants.IterationMarker.r.findAllMatchIn(report).length

    assert(iterations == iterationsToComplete)
  }

  "hierarchicalClusteringWorker::mainLoop" should "complete with empty iteration on empty graphs" in {
    executeTest(emptyEdgeList, 1)
  }

  "hierarchicalClusteringWorker::mainLoop" should "complete in 1 iteration for connected graphs" in {
    executeTest(edgeListOneIteration, 1)
  }

  "hierarchicalClusteringWorker::mainLoop" should "complete in 2 iterations for connected graphs" in {
    executeTest(edgeListTwoIterations, 2)
  }

  "hierarchicalClusteringWorker::mainLoop" should "complete in 3 iterations for connected graphs" in {
    executeTest(edgeListThreeIterations, 3)
  }

  "hierarchicalClusteringWorker::mainLoop" should "complete in 4 iterations for connected graphs" in {
    executeTest(edgeListFourIterations, 4)
  }
}
