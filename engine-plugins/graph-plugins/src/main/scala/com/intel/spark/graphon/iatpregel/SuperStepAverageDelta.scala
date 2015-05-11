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

package com.intel.spark.graphon.iatpregel

import org.apache.spark.rdd.RDD
import akka.dispatch.sysmsg.Failed

/**
 * Provides a method for creating an initial report that summarizes vertex and edge counts.
 *
 * @tparam V Class of the vertex data in the graph.
 * @tparam E Class of the edge data in the graph.
 */
class BasicCountsInitialReport[V, E] extends InitialReport[V, E] with Serializable {

  private def vertexToCount(v: V): Long = 1
  private def edgeToCount(e: E): Long = 1

  /**
   * @param vertices RDD of the per-vertex data.
   * @param edges RDD of the per-edge data.
   * @return Initial report that summarizes vertex and edge counts.
   */
  def generateInitialReport(vertices: RDD[V], edges: RDD[E]): String = {
    "Vertex Count: " + vertices.count() + "\nEdge Count: " + edges.count() + "\n"
  }

}

/**
 * Aggregater for per-superstep status reports.
 * @param activeCount Number fo active vertices
 * @param sumOfDeltas Net change.
 */
case class SuperStepNetDelta(activeCount: Long, sumOfDeltas: Double) extends Serializable

/**
 * Provides a method for generating per-superstep reports that summarizes the vertex count and average change per
 * vertex since the last superstep.
 *
 * @tparam V Class of the vertex data.
 */

class AverageDeltaSuperStepStatusGenerator[V <: DeltaProvider](val convergenceThreshold: Double)
    extends SuperStepStatusGenerator[V] with Serializable {

  private def accumulateSuperStepStatus(status1: SuperStepNetDelta, status2: SuperStepNetDelta) = {
    new SuperStepNetDelta(status1.activeCount + status2.activeCount, status1.sumOfDeltas + status2.sumOfDeltas)
  }

  private def convertVertexDataToStatus(v: V) = SuperStepNetDelta(activeCount = 1, sumOfDeltas = v.delta)

  /**
   *
   * @param iteration Number of the Pregel superstep that has just completed.
   * @param activeVertices RDD of the per-vertex data.
   * @return Summary of the vertex count and average change per vertex since the last superstep.
   */
  def generateSuperStepStatus(iteration: Int, totalVertexCount: Long, activeVertices: RDD[V]) = {

    val emptyStatus = SuperStepNetDelta(0, 0)

    val status = activeVertices.map(v => convertVertexDataToStatus(v)).fold(emptyStatus)(accumulateSuperStepStatus)

    val earlyTermination = (status.sumOfDeltas / totalVertexCount) <= convergenceThreshold

    val log =
      "IATPregel engine has completed iteration " + iteration + "  " + ".  There were " + status.activeCount +
        " many active vertices. The average delta was " + (status.sumOfDeltas / totalVertexCount) + "\n"

    SuperStepStatus(log, earlyTermination)
  }

}
