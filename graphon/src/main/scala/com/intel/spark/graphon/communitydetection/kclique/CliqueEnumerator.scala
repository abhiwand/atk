
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

import com.intel.spark.graphon.communitydetection.kclique.DataTypes._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * K-Clique enumeration
 * One of the core component of K-Clique Community detection Algorithm
 * The algorithm is described in the paper:
 * "Distributed Clique Percolation based Community Detection on Social Networks using MapReduce" by
 * Varamesh, A.; Akbari, M.K. ; Fereiduni, M. ; Sharifian, S. ; Bagheri, A.
 * The paper is available at
 * http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=6620116
 */

object CliqueEnumerator {

  /**
   * Transform graph from adjacency list representation to edge list representation.
   *
   * @param vertices RDD of vertices in adjacency list format.
   * @return Edge set representation of the graph
   */

  def createEdgeListFromParsedAdjList(vertices: RDD[VertexInAdjacencyFormat]): RDD[Edge] = {
    val edgeListWithDuplicates = vertices.flatMap(vertex => {
      vertex.neighbors.map(nbr => Edge.edgeFactory(vertex.id, nbr))
    })

    val distinctEdgeList = edgeListWithDuplicates.distinct()
    distinctEdgeList
  }

  /**
   * Derive the 1 clique-extension facts from the edge list, which means to gather
   * the neighbors of the source vertices into an adjacency list (using sets),
   * which will provide the starting point for later expansion as we add more connected
   * vertices.
   *
   * Notice that the invariant holds:
   *
   * k is odd, and every vertex ID in the VertexSet is less than every vertex ID in the ExtendersSet.
   *
   * @param edgeList Edge list representation of the graph.
   * @return RDD of extended-by facts.
   */

  def initialExtendByMappingFrom(edgeList: RDD[Edge]): RDD[ExtendersFact] = {
    //A map of source vertices to a set of destination vertices connected from the source
    val initMap = edgeList.groupBy(_.source).mapValues(_.map(_.destination).toSet)

    //A map of singleton sets (containing source vertices) to the set of neighbors -
    //essentially an adjacency list
    initMap.map(vToVListMap => ExtendersFact(CliqueFact(Set(vToVListMap._1)), vToVListMap._2, neighborsHigh = true))
  }

  /**
   * Generate all the k+1-cliques from an RDD of k-cliques and their extensions
   *
   * @param extensionFacts the k-cliques and extenders
   * @return an RDD of k+1 cliques
   */
  def deriveKCliquesFromKMinusOneExtensions(extensionFacts: RDD[ExtendersFact]): RDD[CliqueFact] = {
    extensionFacts.flatMap(extendClique)
  }

  /**
   * Generate all the k+1-cliques from a k-clique and a set of vertices that extend it (are connected to all
   * vertices in the k-clique)
   *
   * @param extendersFact the k-clique and the vertices that are connected to every vertex in the k-clique
   * @return a k+1 clique
   */
  def extendClique(extendersFact: ExtendersFact): Set[CliqueFact] = {
    extendersFact match {
      case ExtendersFact(clique, extenders, neighborHigh: Boolean) =>
        extenders.map(extendByVertex => CliqueFact(clique.members + extendByVertex))
    }
  }

  /**
   * Derive neighbors-of facts from an extends-by fact.
   *
   * INVARIANT:
   * when verticesLessThanNeighbor, every vertex ID in the clique members is less than the vertex ID in the resulting
   * NeighborsOfFact. Otherwise, every vertex ID in the clique members is greater than the vertex ID in the resulting
   * NeighborsOfFact.
   *
   *
   * @param extensionFacts RDD of ExtendersFacts from round k-1
   * @return The neighbors-of facts for this extender fact
   */
  def deriveNeighborsFromExtensions(extensionFacts: RDD[ExtendersFact],
                                    verticesLessThanNeighbor: Boolean): RDD[NeighborsOfFact] = {
    extensionFacts.flatMap(deriveNeighbors)
  }

  /**
   * Derive neighbors-of facts from an extends-by fact.
   *
   * INVARIANT:
   * when verticesLessThanNeighbor, every vertex ID in the clique members is less than the vertex ID in the resulting
   * NeighborsOfFact. Otherwise, every vertex ID in the clique members is greater than the vertex ID in the resulting
   * NeighborsOfFact.
   *
   *
   * @param extenderFact ExtendersFact from round k-1
   * @return The neighbors-of facts for this extender fact
   */
  def deriveNeighbors(extenderFact: ExtendersFact): Iterator[NeighborsOfFact] = {
    extenderFact match {
      case ExtendersFact(clique, extenders: VertexSet, _) =>

        val twoSetsFromExtenders = extenders.subsets(2)

        if (extenderFact.neighborsHigh) {
          val minimumCliqueMember = clique.members.min
          twoSetsFromExtenders.map(subset =>
            NeighborsOfFact(subset ++ (clique.members - minimumCliqueMember), minimumCliqueMember, neighborHigh = false))
        }
        else {
          val maximumCliqueMember = clique.members.max
          twoSetsFromExtenders.map(subset =>
            NeighborsOfFact(subset ++ (clique.members - maximumCliqueMember), maximumCliqueMember, neighborHigh = true))
        }
    }
  }

  /**
   * Combines clique facts and neighborsof facts into extended-by facts.
   *
   * INVARIANT:
   * when k is odd, every vertex ID in the VertexSet is less than every vertex ID in the ExtendersSet.
   * when k is even, every vertex ID in the VertexSet is greater than every vertex ID in the ExtenderSet.
   *
   * This invariant is inherited from the k neighbors-of facts.
   *
   * @param cliques The set of k-cliques in the graph.
   * @param neighborFacts The set of neighbors-of facts of k-sets in the graph.
   * @return Set of (k+1) clique extension facts.
   */
  def deriveNextExtensionsFromCliquesAndNeighbors(cliques: RDD[CliqueFact], neighborFacts: RDD[NeighborsOfFact]): RDD[ExtendersFact] = {

    //Map cliques to key-value pairs where the key is the vertex set and the value is a 0. Don't care
    //about the zero because it's just to get us into a pair so we can call cogroup later.
    val cliquesAsPairs: RDD[(VertexSet, Int)] = cliques.map({ case CliqueFact(members) => (members, 0) })

    //Map neighbors to key-value pairs where the key is the vertex set and the value is a pair
    //of the neighbor and the "neighborHigh" flag. These flags will be the same for every pair,
    //so a later optimization might be able to eliminate that part.
    val neighborsAsPairs = neighborFacts.map({
      case NeighborsOfFact(members, neighbor, neighborHigh) =>
        (members, (neighbor, neighborHigh))
    })

    //Cogroups (join, then group by) cliques and neighbor sets by identical vertex sets (the keys of the two RDDs
    //above). The "cliqueTags" are a Seq of zeroes. We care about that for the filter that comes next.
    val cliquesAndNeighborsCoGrouped = cliquesAsPairs.cogroup(neighborsAsPairs)
      .map({
        case (members, (cliqueTags, neighbors)) =>
          (members, cliqueTags, neighbors)
      })

    //remove vertex sets that don't have cliques or don't have neighbors - these don't make it to the next round.
    val filteredCoGroups = cliquesAndNeighborsCoGrouped.filter(
      {
        case (members, cliqueTags, neighbors) =>
          cliqueTags.length > 0 && neighbors.length > 0
      })

    //Repackage these tuples as ExtenderFacts
    val cliquesAndNeighbors = filteredCoGroups.map({
      case (members, cliqueTags, neighbors) =>
        val (neighborVertices, neighborHighs) = neighbors.unzip(identity)
        ExtendersFact(CliqueFact(members),
          neighborVertices.toSet,
          neighborHighs.head)
    })

    cliquesAndNeighbors
  }

  /**
   * Driver for the k-clique percolation algorithm.
   *
   * @param data The RDD of vertices in adjacency list format
   * @param K The clique enumeration number to find K-Cliques
   * @return An RDD of extended-by facts.
   */
  def applyToAdjacencyList(data: RDD[VertexInAdjacencyFormat], K: Int) = {

    val verticesInAdjListForm: RDD[VertexInAdjacencyFormat] = data

    val edgeList: RDD[Edge] = createEdgeListFromParsedAdjList(verticesInAdjListForm)

    applyToEdgeList(edgeList, K)
  }

  def applyToEdgeList(edgeList: RDD[Edge], K: Int): RDD[ExtendersFact] = {
    /**
     * Derive  k-clique-extension facts from (k-1) clique extension facts.
     * @param k  The sizes of the cliques under consideration, ie. the current iteration of the algorithm.
     * @param edgeList The edge list of the underlying graph.
     * @return The k clique extension facts.
     */

    def cliqueExtension(k: Int, edgeList: RDD[Edge]): RDD[ExtendersFact] = {
      if (k == 1) {
        initialExtendByMappingFrom(edgeList)
      }
      else {

        val kMinusOneExtensionFacts = cliqueExtension(k - 1, edgeList)

        val kCliques = deriveKCliquesFromKMinusOneExtensions(kMinusOneExtensionFacts)

        val kNeighborsOfFacts = deriveNeighborsFromExtensions(kMinusOneExtensionFacts, k % 2 == 1)

        deriveNextExtensionsFromCliquesAndNeighbors(kCliques, kNeighborsOfFacts)
      }
    }

    cliqueExtension(K - 1, edgeList)
  }
}
