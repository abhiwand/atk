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

import com.intel.spark.graphon.communitydetection.kclique.datatypes.Datatypes.VertexSet
import com.intel.spark.graphon.connectedcomponents.ConnectedComponentsGraphXDefault
import com.intel.spark.graphon.idassigner._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Assign new Long IDs for each K-cliques of the k-clique graphs. Create a new graph using these Long IDs as
 * new vertices and run connected components to get communities
 */

object GetConnectedComponents extends Serializable {

  /**
   * Run the connected components and get the mappings of cliques to component IDs.
   *
   * @return RDD of pairs of (clique, community ID) where each ID is the component of the clique graph to which the clique
   *         belongs.
   */
  def run(cliqueGraphVertices: RDD[VertexSet], cliqueGraphEdges: RDD[(VertexSet, VertexSet)]): RDD[(VertexSet, Long)] = {

    //    Generate new Long IDs for each K-Clique in k-clique graph. These long IDs will be the vertices
    //    of a new graph. In this new graph, the edge between two vertices will exists if the two original
    //    k-cliques corresponding to the two vertices have exactly (k-1) number of elements in common
    val graphIDAssigner = new GraphIDAssigner[VertexSet]()
    val graphIDAssignerOutput = graphIDAssigner.run(cliqueGraphVertices, cliqueGraphEdges)
    val cliqueIDsToCliques = graphIDAssignerOutput.newIdsToOld

    // Get the vertices of the k-clique graph
    val renamedVerticesOfCliqueGraph = graphIDAssignerOutput.vertices

    // Get the edges of the k-clique graph
    val renamedEdgesOfCliqueGraph = graphIDAssignerOutput.edges

    // Get the pair of the new vertex Id and the corresponding set of k-clique vertices
    val newVertexIdToOldVertexIdOfCliqueGraph: RDD[(Long, VertexSet)] = graphIDAssignerOutput.newIdsToOld

    // Run the connected components of the new k-clique graph
    val cliqueIdToCommunityId: RDD[(Long, Long)] =
      ConnectedComponentsGraphXDefault.run(renamedVerticesOfCliqueGraph, renamedEdgesOfCliqueGraph)

    val cliqueToCommunityID: RDD[(VertexSet, Long)] = cliqueIDsToCliques.join(cliqueIdToCommunityId).map(_._2)

    cliqueToCommunityID
  }

  /**
   * Return value of connected components including the community IDs
   * @param connectedComponents pair of new vertex ID and community ID
   * @param newVertexIdToOldVertexIdOfCliqueGraph mapping between new vertex ID and original k-cliques
   */
  case class ConnectedComponentsOutput(connectedComponents: RDD[(Long, Long)], newVertexIdToOldVertexIdOfCliqueGraph: RDD[(Long, VertexSet)])

}
