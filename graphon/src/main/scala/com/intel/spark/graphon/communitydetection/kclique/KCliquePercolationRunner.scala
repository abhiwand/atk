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

package com.intel.spark.graphon.communitydetection.kclique

import com.intel.graphbuilder.elements.{ GBEdge, GBVertex }
import com.intel.spark.graphon.communitydetection.kclique.datatypes.Datatypes.VertexSet
import com.intel.spark.graphon.communitydetection.kclique.datatypes.{ CliqueExtension, Edge }
import org.apache.spark.rdd.RDD

/**
 * The driver for running the k-clique percolation algorithm
 */
object KCliquePercolationRunner {

  /**
   * The main driver to execute k-clique percolation algorithm.
   *
   * The input is a graph, the community cohesiveness parameter (a. k. a. cliqueSize), and the name of a property
   * to which to write output.
   *
   * The output is a new graph (as an edge RDD and a vertex RDD) in which each vertex has had the new
   * property filled with a list containing the ID of each community to which that vertex belongs.
   * A vertex belonging to no community receives the empty list. There is no rule about the generation of
   * community names save that they are Longs drawn from 1 to the number of communities.
   *
   * The algorithm is based on the MapReduce implementation of k clique percolation
   * described in:
   *
   * "Distributed Clique Percolation based community detection on social networks using MapReduce"
   * by Varamesh, A., Akbari, M. K., Fereiduni, M., Sharifian, S., Bagheri, A.
   * 5th Conference on Information and Knowledge Technology, May 2013, pages 478 - 483.
   *
   * Link to paper in IEEE Xplore:
   * [[http://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6620116]]
   *
   * At a high level, the steps of the algorithm are:
   *
   * 1. enumerate all  pairs (C,V) where:
   *   - C is a (k-1) clique.
   *   - For every v in V,  C + v is a k clique.
   *   - For all v so that C + v is a k clique, v is in V.
   * 2. Using this enumeration, construct a graph whose connected components define communities on the
   *    k cliques of the input graph.
   * 3. For every vertex, the set of communities to which it belongs is the set communities of the k cliques
   *    to which the vertex belongs.
   *
   *
   * @param inVertices An RDD of the vertices of the graph to be analyzed.
   * @param inEdges An RDD of the edges of the graph to be analyzed.
   * @param cliqueSize Parameter determining clique-size used to determine communities. Must be at least 2.
   *                   Large values of cliqueSize result in fewer, smaller communities that are more connected
   * @param communityPropertyLabel name of the community property of vertex that will be
   *                               updated/created in the input graph
   */
  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], cliqueSize: Int, communityPropertyLabel: String): (RDD[GBVertex], RDD[GBEdge]) = {

    // Convert ATK edges into the K Clique Percolation internal edge format

    val edgeList: RDD[Edge] = edgeListFromGBEdgeList(inEdges)

    /* Get a list of all ways to extend a k-1 clique into a k clique.
      In particular:
      enumerate all  pairs (C,V) where:
      - C is a (k-1) clique.
      - For every v in V,  C + v is a k clique.
      - For all v so that C + v is a k clique, v is in V.
     */

    val kMinusOneExtensions: RDD[CliqueExtension] = CliqueExtensionEnumerator.run(edgeList, cliqueSize)

    /*
      Construct the clique-shadow graph that will be used to assign communities to the k cliques
      A clique shadow graph is a bipartite graph whose vertex sets are:
      - The k cliques in the input graph.
      - All k-1 subsets of k cliques in the input graph (the "shadows" of the cliques in combinatorial parlance)
      There is an edge from a clique to each shadow that it contains as a subset.
     */

    val cliqueShadowGraph = CliqueShadowGraphGenerator.run(kMinusOneExtensions)

    // Run connected component analysis to get the mapping of cliques to communities.
    // Communities are just connected components in the clique-shadow graph.

    val cliquesToCommunities: RDD[(VertexSet, Long)] =
      GetConnectedComponents.run(cliqueShadowGraph.vertices, cliqueShadowGraph.edges)

    // Pair each vertex with a set of the communities to which it belongs... A vertex belongs to a community of the
    // clique graph if it belongs to a clique that belongs to that community.
    val verticesToCommunityLists: RDD[(Long, Set[Long])] = VertexCommunityAssigner.run(cliquesToCommunities)

    // Set the vertex Ids as required by Graph Builder
    // A Graph Builder vertex is described by three components -
    //    a unique Physical ID (in this case this vertex Id)
    //    a unique gb Id, and
    //    the properties of vertex (in this case the community property)
    val gbVertexRDDBuilder: GBVertexRDDBuilder = new GBVertexRDDBuilder(inVertices, verticesToCommunityLists)
    val newGBVertices: RDD[GBVertex] = gbVertexRDDBuilder.setVertex(communityPropertyLabel)

    (newGBVertices, inEdges)
  }

  /**
   * Convert ATK edge to the K Clique Percolation internal edge format
   * [[com.intel.spark.graphon.communitydetection.kclique.datatypes.Edge]]
   *
   * @param gbEdgeList RDD of edges in ATK common format.
   * @return An RDD of the edges as [[com.intel.spark.graphon.communitydetection.kclique.datatypes.Edge]]
   */
  def edgeListFromGBEdgeList(gbEdgeList: RDD[GBEdge]): RDD[Edge] = {

    gbEdgeList.filter(e => e.tailPhysicalId.asInstanceOf[Long] < e.headPhysicalId.asInstanceOf[Long]).
      map(e => datatypes.Edge(e.tailPhysicalId.asInstanceOf[Long], e.headPhysicalId.asInstanceOf[Long]))
  }
}
