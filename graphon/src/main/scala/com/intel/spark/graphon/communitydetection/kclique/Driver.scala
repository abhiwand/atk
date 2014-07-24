
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
import com.intel.graphbuilder.elements.{ Edge => GBEdge, Vertex => GBVertex }
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import org.apache.spark.SparkContext
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.spark.graphon.communitydetection.kclique.datatypes.Edge

/**
 * The driver for running the k-clique percolation algorithm
 */
object Driver {

  /**
   * The main driver to execute k-clique percolation algorithm
   * @param titanConfig The titan configuration for input
   * @param sc SparkContext
   * @param cliqueSize Parameter determining clique-size used to determine communities. Must be at least 2.
   *                   Large values of cliqueSize result in fewer, smaller communities that are more connected
   * @param communityPropertyLabel name of the community property of vertex that will be
   *                               updated/created in the input graph
   */
  def run(titanConfig: SerializableBaseConfiguration, sc: SparkContext, cliqueSize: Int, communityPropertyLabel: String) = {

    // Create the Titan connection
    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read the graph from Titan
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    // Get the GraphBuilder vertex list
    val gbVertices = titanReaderRDD.filterVertices()

    // Get the GraphBuilder edge list
    val gbEdges = titanReaderRDD.filterEdges()

    // Convert the graph builder edge list to the edge list as a pair of vertices Ids
    // that can be used in KClique Percolation
    val edgeList = edgeListFromGBEdgeList(gbEdges)

    // Get all enumerated K-Cliques using the edge list
    val enumeratedKCliques = CliqueEnumerator.run(edgeList, cliqueSize)

    // Construct the clique graph that will be input for connected components
    val kCliqueGraphGeneratorOutput = GraphGenerator.run(enumeratedKCliques)

    // Run connected component analysis to get the cliques and corresponding communities
    val cliquesAndConnectedComponent = GetConnectedComponents.run(kCliqueGraphGeneratorOutput, sc)

    // Pair each vertex with a set of the communities to which it belongs
    val vertexCommunitySet =
      CommunityAssigner.run(cliquesAndConnectedComponent.connectedComponents, cliquesAndConnectedComponent.newVertexIdToOldVertexIdOfCliqueGraph)

    // Set the vertex Ids as required by Graph Builder
    // A Graph Builder vertex is described by three components -
    //    a unique Physical ID (in this case this vertex Id)
    //    a unique gb Id, and
    //    the properties of vertex (in this case the community property)
    val gbVertexRDDBuilder: GBVertexRDDBuilder = new GBVertexRDDBuilder(gbVertices, vertexCommunitySet)
    val newGBVertices: RDD[GBVertex] = gbVertexRDDBuilder.setVertex(communityPropertyLabel)

    // Update back each vertex in the input Titan graph and the write the community property
    // as the set of communities to which it belongs
    val communityWriterInTitan = new CommunityWriterInTitan()
    communityWriterInTitan.run(newGBVertices, gbEdges, titanConfig)

  }

  /**
   * Convert Graph Builder edge to the undirected edge as pair of vertex identifiers
   * @param gbEdgeList Graph Builder edge list
   * @return an RDD of Edge set
   */
  def edgeListFromGBEdgeList(gbEdgeList: RDD[GBEdge]): RDD[Edge] = {

    gbEdgeList.filter(e => (e.tailPhysicalId.asInstanceOf[Long] < e.headPhysicalId.asInstanceOf[Long])).
      map(e => datatypes.Edge(e.tailPhysicalId.asInstanceOf[Long], e.headPhysicalId.asInstanceOf[Long]))
  }
}
