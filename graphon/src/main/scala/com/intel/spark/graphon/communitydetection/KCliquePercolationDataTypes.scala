
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

/**
 * The data types that has been used for k-clique percolation algorithm
 */
object KCliquePercolationDataTypes {

	/**
	 * Represents a vertex in adjacency list form
	 *
	 * @param id The unique identifier of this vertex.
	 * @param neighbors The list of this vertex's neighbors.
	 */
	case class VertexInAdjacencyFormat(id: Long, neighbors: Array[Long]) extends Serializable

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

	/**
	 * A set of vertices, as manipulated by the k-clique percolation algorithm.
	 * These sets should contain no more than k vertices.
	 */
	type VertexSet = Set[Long]

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

  case class CliqueFact(members: VertexSet) extends Serializable

	/**
	 * Encodes the fact that a given VertexSet forms a clique, and that the clique can be extended by adding
	 * any one of the vertices from the ExtendersSet.
	 *
	 * A k clique-extension fact is a clique extension fact where the vertex set contains exactly k vertices.
	 * These are the extension facts obtained after the k'th round of the algorithm.
	 *
	 * INVARIANT:
	 * when k is odd, every vertex ID in the VertexSet is less than every vertex ID in the ExtendersSet.
	 * when k is even, every vertex ID in the VertexSet is greater than every vertex ID in the ExtenderSet.
	 *
	 */
	case class ExtendersFact(clique: CliqueFact, neighbors: VertexSet, neighborsHigh: Boolean) extends Serializable

}