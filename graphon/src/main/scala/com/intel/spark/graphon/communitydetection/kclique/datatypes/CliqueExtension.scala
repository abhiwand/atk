
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

import com.intel.spark.graphon.communitydetection.kclique.datatypes.Datatypes.VertexSet

/**
 * Encodes the fact that a given VertexSet forms a clique, and that the clique can be extended by adding
 * any one of the vertices from the set neighbors.
 *
 * A k clique-extension fact is a clique extension fact where the vertex set contains exactly k vertices.
 * These are the extension facts obtained after the k'th round of the algorithm.
 *
 * Symbolically, a pair (C,V) where:
 *   - C is a (k-1) clique.
 *   - For every v in V,  C + v is a k clique.
 *   - For all v so that C + v is a k clique, v is in V.
 *
 * INVARIANT:
 * when k is odd, every vertex ID in the VertexSet is less than every vertex ID in the ExtendersSet.
 * when k is even, every vertex ID in the VertexSet is greater than every vertex ID in the ExtenderSet.
 *
 */
case class CliqueExtension(clique: Clique, neighbors: VertexSet, neighborsHigh: Boolean) extends Serializable
