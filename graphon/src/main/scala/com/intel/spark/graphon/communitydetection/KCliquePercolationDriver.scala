
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

import com.intel.spark.graphon.titanreader.TitanReader
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Edge => GBEdge, GraphElement }
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._
import com.intel.spark.graphon.communitydetection.CreateGraphFromEnumeratedKCliques.KCliqueGraphOutput
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.SparkContext
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.graph.titan.TitanGraphConnector

/**
 * The driver for running the k-clique percolation algorithm
 */
object KCliquePercolationDriver {

  /**
   * The main driver to execute k-clique percolation algorithm
   * @param graphTableName This should be reference to the graph from which we are pulling data
   * @param titanStorageHostName
   * @param sparkMaster
   * @param sparkContext SparkContext
   * @param cliqueSize Parameter determining clique-size used to determine communities. Must be at least 1.
   *                   Large values of cliqueSize result in fewer, smaller communities that are more connected
   * @param communityPropertyDefaultLabel name of the community property of vertex that will be updated/created in the input graph
   */
  def run(graphTableName: String, titanStorageHostName: String, sparkMaster: String, sparkContext: SparkContext, cliqueSize: Int, communityPropertyDefaultLabel: String) = {

    //    Load the graph from Titan
    //    Check whether we should pass titanConnector instead of graphTableName, titanStorageHostName and sparkMaster
    val graphElements: RDD[GraphElement] =
      new TitanReader().loadGraph(graphTableName: String, titanStorageHostName: String, sparkMaster: String)

    //    Get the GraphBuilder edge list
    val gbEdgeList = graphElements.filterEdges()

    //    Convert the graph builder edge list to the edge list that can be used in KClique Percolation
    val edgeList: RDD[Edge] = edgeListFromGBEdgeList(gbEdgeList)

    //    Get all enumerated K-Cliques using the edge list
    val enumeratedKCliques: RDD[ExtendersFact] = KCliqueEnumeration.applyToEdgeList(edgeList, cliqueSize)

    //    Construct the clique graph that will be input for connected components
    val kcliqueGraphForComponentAnalysis: KCliqueGraphOutput = CreateGraphFromEnumeratedKCliques.run(enumeratedKCliques)

    //    Run connected component analysis to get the communities
    val cliquesAndConnectedComponent = GetConnectedComponents.run(kcliqueGraphForComponentAnalysis, sparkContext)

    //    Associate each vertex with a list of the communities to which it belongs
    val vertexAndCommunityList: RDD[(Long, Set[Long])] =
      AssignCommunitiesToVertex.run(cliquesAndConnectedComponent.connectedComponents, cliquesAndConnectedComponent.newVertexIdToOldVertexIdOfCliqueGraph)

    // nls test - this is just an example to demonstrate that the json converter can be used
    val testSet = Set(1.toLong, 2.toLong, 3.toLong, 4.toLong, 5.toLong)
    val testSetJson = testSet.toJson
    // end nls test

    //    Create graph connection
    //    We should replace this dependency and pass the titan connector rather than hard code it here
    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", "hbase") // TODO : we should probably have this come from... somewhere
    titanConfig.setProperty("storage.hostname", titanStorageHostName)
    titanConfig.setProperty("storage.tablename", graphTableName)
    val titanConnector = new TitanGraphConnector(titanConfig)

    //    Write back to each vertex in Titan graph the set of communities to which it belongs in the property with name "communities"
    val writeBackToTitan = new WriteBackToTitan()
    writeBackToTitan.run(vertexAndCommunityList, communityPropertyDefaultLabel, titanConnector)

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
