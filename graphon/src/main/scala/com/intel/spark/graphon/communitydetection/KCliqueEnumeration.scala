
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

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._

/**
 * K-Clique enumeration
 * One of the core component of K-Clique Community detection Algorithm
 * The algorithm is described in the paper:
 * "Distributed Clique Percolation based Community Detection on Social Networks using MapReduce" by
 * Varamesh, A.; Akbari, M.K. ; Fereiduni, M. ; Sharifian, S. ; Bagheri, A.
 * The paper is available at
 * http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=6620116
 * 
 * @param data The RDD of vertices in adjacency list format 
 * @param K The clique enumeration number to find K-Cliques
 */

class KCliqueEnumeration(data: RDD[VertexInAdjacencyFormat], K: Int) {
	
	val sc = data.sparkContext

	/**
	 * Transform graph from adjacency list representation to edge list representation.
	 *
	 * @param vertices RDD of vertices in adjacency list format.
	 * @return Edge set representation of the graph
	 */

	def createEdgelistFromParsedAdjList(vertices: RDD[VertexInAdjacencyFormat]): RDD[Edge] = {
		val edgelistWithDuplicates = vertices.flatMap(vertex => {
			vertex.neighbors.map(nbr => Edge.edgeFactory(vertex.id, nbr))
		})
		
		val distinctEdgeList = edgelistWithDuplicates.distinct()
		return distinctEdgeList
	}

	/**
	 * Derive the 1 clique-extension facts from the edge list.
	 *
	 * Notice that the invariant holds:
	 *
	 * k is odd, and every vertex ID in the VertexSet is less than every vertex ID in the ExtendersSet.
	 *
	 * @param edgelist Edge list representation of the graph.
	 * @return RDD of extended-by facts.
	 */

	def initialExtendByMappingFrom(edgelist: RDD[Edge]): RDD[ExtendersFact] = {
		val initMap = edgelist.groupBy(_.source).mapValues(_.map(_.destination).toSet)
		initMap.map(vToVlistMap => (Set(vToVlistMap._1), ExtendersSet(vToVlistMap._2)))
	}

	/**
	 * Derive clique facts from a k-1 extends-by facts.
	 * @param extensionFacts
	 * @return
	 */

	def deriveKCliquesFromKMinusOneExtensions(extensionFacts: RDD[ExtendersFact]): RDD[CliqueFact] = {
		extensionFacts.flatMap(
			{
				case (keySet: Set[Long], extendersFact: ExtendersSet) => {
					val extendingVertices = extendersFact.extendingVertices
					extendingVertices.map(extendByVertex => CliqueFact(keySet + extendByVertex))
				}
			})
	}

	/**
	 * Derive a neighbors-of facts from an extends-by facts.
	 *
	 * INVARIANT:
	 * when k is odd, every vertex ID in the VertexSet is less than the vertex ID in NeighborsOf.v
	 * when k is even, every vertex ID in the VertexSet is greater than the vertex ID in NeighborsOf.v
	 *
	 *
	 * @param kMinusOneExtensionFacts RDD of ExtendersFact's from round k-1
	 * @param k Size of the vertex sets being analyzed.
	 * @return The kth round neighbors-of facts.
	 */
	def deriveKNeighborsOfFromKMinusOneExtensions(kMinusOneExtensionFacts: RDD[ExtendersFact], k: Int): RDD[NeighborsOfFact] = {
		kMinusOneExtensionFacts.flatMap(uToVMap => {
			val U = uToVMap._1
			val V = uToVMap._2.extendingVertices

			val vwPairFromV = V.subsets(2)

			// notice that when k-1 is even, v and w are smaller than every vertex of U
			// and when k-1 is odd, v and w are larger than every vertex of U

			val UMax = U.max
			val UMin = U.min

			if (k % 2 == 1) {
				vwPairFromV.map(vwPair => ((vwPair ++ (U - UMin)), NeighborsOf(UMin)))
			} else {
				vwPairFromV.map(vwPair => ((vwPair ++ (U - UMax)), NeighborsOf(UMax)))
			}
		})
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
	 * @param neighbors The set of neighbors-of facts of k-sets in the graph.
	 * @return Set of (k+1) clique extension facts.
	 */
	def deriveKExtensionFactsFromKCliqueAndKNeighborsFacts(cliques: RDD[(VertexSet, Clique)], neighbors: RDD[(VertexSet, NeighborsOf)]): RDD[ExtendersFact] = {

		val cliquesAndNeighborsCoGrouped : RDD[(VertexSet, (Seq[Clique], Seq[NeighborsOf]))] =  cliques.cogroup(neighbors)

		val filteredCoGroups : RDD[(VertexSet, (Seq[Clique], Seq[NeighborsOf]))] = cliquesAndNeighborsCoGrouped.filter(x => (x._2._1.length != 0 && x._2._2.length != 0))

			def helper(neighborfacts: Seq[NeighborsOf]): ExtendersSet = {
				ExtendersSet(neighborfacts.map(neighborFact => neighborFact.v).toSet)
			}
		
		val filteredUniqueCoGroups : RDD[(VertexSet, Seq[(Seq[Clique], Seq[NeighborsOf])])] = filteredCoGroups.groupBy(_._1).mapValues(_.map(_._2))
		
		val vertexsetAndNeighbors : RDD[(VertexSet, Seq[NeighborsOf])] = filteredUniqueCoGroups.mapValues(_.map(_._2)).map{ case (k,Seq(v)) => (k,v) }
		
		val cliqueExtendedByMapping : RDD[ExtendersFact] = vertexsetAndNeighbors.mapValues( {case(neighborFacts: Seq[NeighborsOf]) => helper(neighborFacts)} )
		
		return cliqueExtendedByMapping

	}

	/**
	 * Driver for the k-clique percolation algorithm.
	 * @return An RDD of extended-by facts.
	 */
	def kCliqueExtraction(): RDD[ExtendersFact] = {

		val verticesInAdjListForm: RDD[VertexInAdjacencyFormat] = data

		val edgeList: RDD[Edge] = createEdgelistFromParsedAdjList(verticesInAdjListForm)

			/**
			 * Derive  k-clique-extension facts from (k-1) clique extension facts.
			 * @param k  The sizes of the cliques under consideration, ie. the current iteration of the algorithm.
			 * @param edgeList The edge list of the underlying graph.
			 * @return The k clique extension facts.
			 */

			def cliqueExtension(k: Int, edgeList: RDD[Edge]): RDD[ExtendersFact] = {
				if (k == 1) {
					initialExtendByMappingFrom(edgeList)
				} else {

					val kMinusOneExtensionFacts = cliqueExtension(k - 1, edgeList)

					val kCliques = deriveKCliquesFromKMinusOneExtensions(kMinusOneExtensionFacts)

					val kNeighborsOfFacts = deriveKNeighborsOfFromKMinusOneExtensions(kMinusOneExtensionFacts, k)

					deriveKExtensionFactsFromKCliqueAndKNeighborsFacts(kCliques, kNeighborsOfFacts)
				}
			}

		cliqueExtension(K - 1, edgeList)
	}
}