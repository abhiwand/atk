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

/**
 * This is the hierarchical clustering edge
 * @param src source node
 * @param srcNodeCount 1 if node is leaf, >1 if meta-node
 * @param dest destination node
 * @param destNodeCount 1 if node is leaf, >1 if meta-node
 * @param distance edge distance
 * @param isInternal true if the edge is internal (created through node edge collapse), false otherwise
 */
case class HierarchicalClusteringEdge(var src: Long,
                                      var srcNodeCount: Long,
                                      var dest: Long,
                                      var destNodeCount: Long,
                                      var distance: Float,
                                      isInternal: Boolean) {
  /**
   * Get the total node count of the edge
   * @return sum of src + dest counts
   */
  def getTotalNodeCount(): Long = {
    srcNodeCount + destNodeCount
  }

}
