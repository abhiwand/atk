
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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.kclique.datatypes.{ CliqueExtension, Clique }
import com.intel.spark.graphon.communitydetection.kclique.datatypes.Datatypes.VertexSet

object CliqueShadowGraphGenerator extends Serializable {

  /**
   * Return value of the CliqueShadowGraphGenerator
   * @param vertices List of vertices of new graph where vertices are k-cliques
   * @param edges List of edges between the vertices of new graph of k-cliques
   */
  case class CliqueShadowGraph(val vertices: RDD[VertexSet],
                               val edges: RDD[(VertexSet, VertexSet)])

  /**
   * Generate the clique-shadow graph from the extension facts.
   *
   *   A clique shadow graph is a bipartite graph whose vertex sets are:
   *   - The k cliques in the input graph.
   *   - All k-1 subsets of k cliques in the input graph (the "shadows" of the cliques in combinatorial parlance)
   *   There is an edge from a clique to each shadow that it contains as a subset.
   *
   * @param cliqueExtensions RDD of clique extension facts
   * @return The clique-shadow graph, packaged into a CliqueShadowGraph
   */
  def run(cliqueExtensions: RDD[CliqueExtension]) = {

    val cliques: RDD[VertexSet] = cliqueExtensions.flatMap(
      { case CliqueExtension(clique, extenders, _) => extenders.map(v => clique.members + v) })

    val cliqueToShadowEdges: RDD[(VertexSet, VertexSet)] = cliques.flatMap(V => (V.subsets(V.size - 1).map(U => (V, U))))

    val shadows: RDD[VertexSet] = cliqueToShadowEdges.map(_._2).distinct()

    val vertices: RDD[VertexSet] = cliques.union(shadows)
    val edges: RDD[(VertexSet, VertexSet)] = cliqueToShadowEdges.flatMap({ case (x, y) => Set((x, y), (y, x)) })

    new CliqueShadowGraph(vertices, edges)
  }

}
