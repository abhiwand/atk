
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

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Edge => GBEdge, Vertex => GBVertex, GraphElement }
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.spark.graphon.communitydetection.KCliqueGraphGenerator._
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader

/**
 * The driver for running the k-clique percolation algorithm
 */
object KCliquePercolationDriver {

  /**
   * The main driver to execute k-clique percolation algorithm
   * @param titanConnector The titan graph connector
   * @param sc SparkContext
   * @param cliqueSize Parameter determining clique-size used to determine communities. Must be at least 1.
   *                   Large values of cliqueSize result in fewer, smaller communities that are more connected
   * @param communityPropertyDefaultLabel name of the community property of vertex that will be updated/created in the input graph
   */
  def run(titanConnector: TitanGraphConnector, sc: SparkContext, cliqueSize: Int, communityPropertyDefaultLabel: String) = {

    //    Read the graph from Titan
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    //    Get the GraphBuilder vertex list
    val gbVertexIDs = titanReaderRDD.filterVertices().map((v: GBVertex) => v.physicalId.asInstanceOf[Long])

    //    Get the GraphBuilder edge list
    val gbEdgeList = titanReaderRDD.filterEdges()

    //    Convert the graph builder edge list to the edge list that can be used in KClique Percolation
    val edgeList: RDD[Edge] = edgeListFromGBEdgeList(gbEdgeList)

    //    Get all enumerated K-Cliques using the edge list
    val enumeratedKCliques: RDD[ExtendersFact] = KCliqueEnumeration.applyToEdgeList(edgeList, cliqueSize)

    //    Construct the clique graph that will be input for connected components
    val kCliqueGraphGeneratorOutput: KCliqueGraphGeneratorOutput = KCliqueGraphGenerator.run(enumeratedKCliques)

    //    Run connected component analysis to get the communities
    val cliquesAndConnectedComponent = GetConnectedComponents.run(kCliqueGraphGeneratorOutput, sc)

    //    Define an empty set of Long
    val emptySet: Set[Long] = Set()

    //    Map the GB Vertex IDs to key-value pairs where the key is the GB Vertex ID set and the value is the emptySet.Don't care
    //    The empty set will be considered as the empty communities for the vetices that don't belong to any communities.
    val vertexIDEmptySetPairs: RDD[(Long, Set[Long])] = gbVertexIDs.map(id => (id, emptySet))

    //    Associate each vertex with a list of the communities to which it belongs
    val vertexAndCommunitySet: RDD[(Long, Set[Long])] =
      AssignCommunitiesToVertex.run(cliquesAndConnectedComponent.connectedComponents, cliquesAndConnectedComponent.newVertexIdToOldVertexIdOfCliqueGraph)

    //    Combine the vertices having communities with the vertices having no communities together, to have
    //    complete list of vertices
    val vertexAndCommunitiesWithEmptyCommunity: RDD[(Long, Set[Long])] =
        vertexIDEmptySetPairs.union(vertexAndCommunitySet).combineByKey((x => x),
          ({ case (x, y) => y.union(x) }),
          ({ case (x, y) => y.union(x) }))

    //    Write back to each vertex in Titan graph the set of communities to which it belongs in the property with name "communities"
    val kCliqueCommunityWriterInTitan = new KCliqueCommunityWriterInTitan()
    kCliqueCommunityWriterInTitan.run(vertexAndCommunitiesWithEmptyCommunity, communityPropertyDefaultLabel, titanConnector)

  }

  /**
   * Convert Graph Builder edge to the undirected edge as pair of vertex identifiers
   * @param gbEdgeList Graph Builder edge list
   * @return an RDD of Edge set
   */
  def edgeListFromGBEdgeList(gbEdgeList: RDD[GBEdge]): RDD[Edge] = {

    gbEdgeList.filter(e => (e.tailPhysicalId.asInstanceOf[Long] < e.headPhysicalId.asInstanceOf[Long])).
      map(e => KCliquePercolationDataTypes.Edge(e.tailPhysicalId.asInstanceOf[Long], e.headPhysicalId.asInstanceOf[Long]))

  }
}
