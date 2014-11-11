
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

package com.intel.spark.graphon.communitydetection.kclique.datatypes

/**
 * Represents an undirected edge as a pair of vertex identifiers.
 * To avoid duplicate entries of (v,u) and (u,v) for the edge {u,v} we require that the source be less than the
 * destination.
 *
 * @param source Source of the edge.
 * @param destination Destination of the edge.
 */
case class Edge(source: Long, destination: Long) extends Serializable {
  require(source < destination)
}

/**
 * Companion object for Edge class that provides constructor.
 */
object Edge {
  def edgeFactory(u: Long, v: Long) = {
    require(u != v)
    new Edge(math.min(u, v), math.max(u, v))
  }
}

