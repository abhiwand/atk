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
