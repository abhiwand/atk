
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
import com.intel.spark.graphon.communitydetection.kclique.DataTypes._
import org.apache.spark.SparkContext
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.spark.graphon.communitydetection.kclique.GraphGenerator._
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.util.SerializableBaseConfiguration

/**
 * The driver for running the k-clique percolation algorithm
 */
object Driver {

  /**
   * The main driver to execute k-clique percolation algorithm
   * @param titanConfigInput The titan configuration for input
   * @param sc SparkContext
   * @param cliqueSize Parameter determining clique-size used to determine communities. Must be at least 1.
   *                   Large values of cliqueSize result in fewer, smaller communities that are more connected
   * @param communityPropertyDefaultLabel name of the community property of vertex that will be updated/created in the input graph
   */
  def run(titanConfigInput: SerializableBaseConfiguration, sc: SparkContext, cliqueSize: Int, communityPropertyDefaultLabel: String) = {

    //    Create the Titan connection
    val titanConnector = new TitanGraphConnector(titanConfigInput)
    System.out.println("*********Created TitanConnector********")

    //    Read the graph from Titan
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()
    System.out.println("*********Read the graph from Titan********")

    //    Get the GraphBuilder vertex list
    val gbVertices: RDD[GBVertex] = titanReaderRDD.filterVertices()

    //    Get the GraphBuilder edge list
    val gbEdges: RDD[GBEdge] = titanReaderRDD.filterEdges()

    //    Convert the graph builder edge list to the edge list that can be used in KClique Percolation
    val edgeList: RDD[Edge] = edgeListFromGBEdgeList(gbEdges)

    //    Get all enumerated K-Cliques using the edge list
    val enumeratedKCliques: RDD[ExtendersFact] = CliqueEnumerator.applyToEdgeList(edgeList, cliqueSize)

    //    Construct the clique graph that will be input for connected components
    val kCliqueGraphGeneratorOutput: KCliqueGraphGeneratorOutput = GraphGenerator.run(enumeratedKCliques)

    //    Run connected component analysis to get the communities
    val cliquesAndConnectedComponent = GetConnectedComponents.run(kCliqueGraphGeneratorOutput, sc)

    //    Associate each vertex with a list of the communities to which it belongs
    val vertexCommunitySet: RDD[(Long, Set[Long])] =
      CommunityAssigner.run(cliquesAndConnectedComponent.connectedComponents, cliquesAndConnectedComponent.newVertexIdToOldVertexIdOfCliqueGraph)

    val gbVertexSetter: GBVertexSetter = new GBVertexSetter(gbVertices, vertexCommunitySet)
    val newGBVertices: RDD[GBVertex] = gbVertexSetter.setVertex(communityPropertyDefaultLabel)

    //    Write back to each vertex in Titan graph the set of communities to which it belongs in the property with name "communities"
    val communityWriterInTitan = new CommunityWriterInTitan()
    communityWriterInTitan.run(newGBVertices, gbEdges, titanConfigInput)

  }

  /**
   * Convert Graph Builder edge to the undirected edge as pair of vertex identifiers
   * @param gbEdgeList Graph Builder edge list
   * @return an RDD of Edge set
   */
  def edgeListFromGBEdgeList(gbEdgeList: RDD[GBEdge]): RDD[Edge] = {

    gbEdgeList.filter(e => (e.tailPhysicalId.asInstanceOf[Long] < e.headPhysicalId.asInstanceOf[Long])).
      map(e => DataTypes.Edge(e.tailPhysicalId.asInstanceOf[Long], e.headPhysicalId.asInstanceOf[Long]))

  }
}
