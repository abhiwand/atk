
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

package com.intel.spark.graphon.communitydetection.kclique

import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.kclique.datatypes.Datatypes.VertexSet
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import com.intel.spark.graphon.communitydetection.kclique.datatypes._
import com.intel.spark.graphon.connectedcomponents.NormalizeConnectedComponents

/**
 * Get the mapping pair between vertices of each k-cliques and set of community IDs that has been generated from
 * connected components algorithm
 */

object CommunityAssigner extends Serializable {

  /**
   * Emit pairs of vertex ID and community ID from k-clique graph vertex (having a new Long ID) which is originally a set of vertices.
   * Group by vertex IDs so that each vertex ID gets a set (possibly empty) of the community IDs to which it belongs
   *
   */
  def run(cliquesToCommunities: RDD[(VertexSet, Long)]): RDD[(Long, Set[Long])] = {

    val vertexCommunityPairs: RDD[(Long, Long)] =
      cliquesToCommunities.flatMap({ case (clique, communityID) => clique.map(v => (v, communityID)) })

    vertexCommunityPairs.groupByKey().map({ case (vertex, communitySeq) => (vertex, communitySeq.toSet) })

  }

}
