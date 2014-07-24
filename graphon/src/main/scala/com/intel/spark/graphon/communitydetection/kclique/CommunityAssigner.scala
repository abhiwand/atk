
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
import com.intel.spark.graphon.communitydetection.kclique.datatypes._

/**
 * Get the mapping pair between vertices of each k-cliques and set of community IDs that has been generated from
 * connected components algorithm
 */

object CommunityAssigner extends Serializable {

  /**
   * Emit pairs of vertex ID and community ID from k-clique graph vertex (having a new Long ID) which is originally a set of vertices.
   * Group by vertex IDs so that each vertex ID gets a set (possibly empty) of the community IDs to which it belongs
   *
   * @param connectedComponents pair of new Long IDs (corresponding to each k-cliques) and community ID
   * @param cliqueGraphVertexIdToCliqueSet pair of new Long IDs and corresponding k-cliques
   * @return an RDD of pair of original vertex ID and set of community IDs to which it belongs
   */
  def run(connectedComponents: RDD[(Long, Long)], cliqueGraphVertexIdToCliqueSet: RDD[(Long, VertexSet)]): RDD[(Long, Set[Long])] = {

    // Pair of seq of community Id and seq of k-cliques by cogrouping
    // connected components (new vertices and community Id pair) and new vertices Id to
    // old vertices Id (of k-clique) pair
    val seqOfCommunityIdToSeqOfCliques: RDD[(Seq[Long], Seq[Set[Long]])] = connectedComponents.cogroup(cliqueGraphVertexIdToCliqueSet).map(_._2)

    // Get community Id and corresponding set of old vertex Ids of the k-clique
    val communityIdToVertexIdSet: RDD[(Long, Set[Long])] = seqOfCommunityIdToSeqOfCliques.flatMap({
      case (communityIdList, verticesList) =>
        communityIdList.flatMap(communityID =>
          verticesList.map(vertices => (communityID, vertices)))
    })

    // Emit community Id and old vertex vertex Id (of k-clique) pair from the set of vertex Id
    val communityVertexPair = communityIdToVertexIdSet.flatMap(communityIdVertices =>
      communityIdVertices._2.map(vertex =>
        VertexCommunity(vertex, communityIdVertices._1)))

    // group the communities for each k-clique vertices and get pair of vertex Id and set of community Ids
    val vertexCommunitySet: RDD[(Long, Set[Long])] = communityVertexPair.groupBy(_.vertexID).mapValues(_.map(_.communityID).toSet)

    vertexCommunitySet
  }

}
