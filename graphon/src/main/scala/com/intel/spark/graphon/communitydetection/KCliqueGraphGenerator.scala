
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

package com.intel.spark.graphon.communitydetection

import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object KCliqueGraphGenerator extends Serializable {

  def run(cliqueAndExtenders: RDD[ExtendersFact]) = {

    /**
     * Derive the key value pairs of k-1 cliques in the graph and k cliques that extend them
     * and drop the boolean variable neighborHigh that describes whether the neighbors are of higher order
     */
    val cliqueAndExtendedClique: RDD[(CliqueFact, CliqueFact)] =
      cliqueAndExtenders.flatMap({ case ExtendersFact(clique, extenders, neighborHigh) => extenders.map(extendedBy => (CliqueFact(clique.members), CliqueFact(clique.members + extendedBy))) })

    /**
     * Get the distinct CliqueFact set as the new vertex list of k-clique graph
     */
    val cliqueFactVertexList: RDD[CliqueFact] = cliqueAndExtenders.flatMap({ case ExtendersFact(clique, extenders, neighborHigh) => extenders.map(extendedBy => CliqueFact(clique.members + extendedBy)) }).distinct()

    /**
     * Group those pairs by their keys (the k-1) sets, so in each group we get
     * (U, Seq(V_1, . V_m)), where the U is a k-1 clique and each V_i is a k-clique extending it
     */
    val cliqueAndExtendedCliqueSet: RDD[(CliqueFact, Set[CliqueFact])] = cliqueAndExtendedClique.groupBy(_._1).mapValues(_.map(_._2).toSet)

    /**
     * Each V_i becomes a vertex of the clique graph. Create edge list as ( V_i, V_j )
     */
    val pairsOfAdjacentCliques = cliqueAndExtendedCliqueSet.flatMap({ case (clique, setOfCliques) => setOfCliques.subsets(2) })
    val cliqueFactEdgeList = pairsOfAdjacentCliques.map(subset => (subset.head, subset.last))

    val cliqueGraphVertices = cliqueFactVertexList.map({
      case (CliqueFact(cliqueVertex)) => cliqueVertex
    })

    val cliqueGraphEdges: RDD[(VertexSet, VertexSet)] = cliqueFactEdgeList.map({
      case (CliqueFact(idCliqueVertex), CliqueFact(nbrCliqueVertex)) => (idCliqueVertex, nbrCliqueVertex)
    })

    new KCliqueGraphGeneratorOutput(cliqueGraphVertices, cliqueGraphEdges)
  }

  /**
   * Return value of KClique Graph generator
   * @param cliqueGraphVertices List of vertices of new graph where vertices are k-cliques
   * @param cliqueGraphEdges List of edges between the vertices of new graph of k-cliques
   */
  case class KCliqueGraphGeneratorOutput(val cliqueGraphVertices: RDD[VertexSet], val cliqueGraphEdges: RDD[(VertexSet, VertexSet)])

}
