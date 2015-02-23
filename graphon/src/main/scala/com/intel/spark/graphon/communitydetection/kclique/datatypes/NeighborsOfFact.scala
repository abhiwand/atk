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

package com.intel.spark.graphon.communitydetection.kclique.datatypes

import com.intel.spark.graphon.communitydetection.kclique.datatypes.Datatypes.VertexSet
/**
 * Encodes the fact that all vertices of a given vertex set are neighbors of the vertex specified
 * by the NeighborsOf fact.
 *
 * A k neighbors-of fact is a neighbors-of fact in which the VertexSet contains exactly k vertices.
 * These are the kind of neighbors-of facts obtained when proceeding from the round k-1 to round k of the algorithm.
 *
 * INVARIANT:
 * when k is odd, every vertex ID in the VertexSet is less than the vertex ID in NeighborsOf.v
 * when k is even, every vertex ID in the VertexSet is greater than the vertex ID in NeighborsOf.v
 *
 */
case class NeighborsOfFact(members: VertexSet, neighbor: Long, neighborHigh: Boolean) extends Serializable
