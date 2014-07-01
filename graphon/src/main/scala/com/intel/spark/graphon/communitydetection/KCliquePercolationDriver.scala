package com.intel.spark.graphon.communitydetection

import com.intel.spark.graphon.titanreader.TitanReader
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Edge => GBEdge, GraphElement }
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._
import com.intel.spark.graphon.communitydetection.CreateGraphFromEnumeratedKCliques.KCliqueGraphOutput

/**
 * The driver for running the k-clique percolation algorithm
 */
object KCliquePercolationDriver {

  /**
   * Convert Graph Builder edge to the undirected edge as pair of vertex identifiers
   * @param gbEdgeList Graph Builder edge list
   * @return an RDD of Edge set
   */
  def edgeListFromGBEdgeList(gbEdgeList: RDD[GBEdge]): RDD[Edge] = {

    gbEdgeList.filter(e => (e.tailPhysicalId.asInstanceOf[Long] < e.headPhysicalId.asInstanceOf[Long])).
      map(e => KCliquePercolationDataTypes.Edge(e.tailPhysicalId.asInstanceOf[Long], e.headPhysicalId.asInstanceOf[Long]))

  }

  /**
   * The main driver to execute k-clique percolation algorithm
   * @param graphTableName This should be reference to the graph from which we are pulling data
   * @param titanStorageHostName
   * @param sparkMaster
   * @param k Parameter determining clique-size used to determine communities. Must be at least 1. Large values of k
   *          result in fewer, smaller communities that are more connected
   */
  def run(graphTableName: String, titanStorageHostName: String, sparkMaster: String, k: Int) = {

    /**
     * Load the graph from Titan
     */
    val graphElements: RDD[GraphElement] =
      new TitanReader().loadGraph(graphTableName: String, titanStorageHostName: String, sparkMaster: String)

    /**
     * Get the GraphBuilder edge list
     */
    val gbEdgeList = graphElements.filterEdges()

    /**
     * Convert the graph builder edge list to the edge list that can be used in KClique Percolation
     */
    val edgeList: RDD[Edge] = edgeListFromGBEdgeList(gbEdgeList)

    /**
     * Get all enumerated K-Cliques using the edge list
     */
    val enumeratedKCliques: RDD[ExtendersFact] = KCliqueEnumeration.applyToEdgeList(edgeList, k)

    /**
     * Construct the clique graph that will be input for connected components
     */
    val kcliqueGraphForComponentAnalysis: KCliqueGraphOutput = CreateGraphFromEnumeratedKCliques.run(enumeratedKCliques)

    /**
     *  Run connected component analysis to get the communities
     */
    val cliquesAndConnectedComponent = GetConnectedComponents.run(kcliqueGraphForComponentAnalysis)

    /**
     * Associate each vertex with a list of the communities to which it belongs
     */
    val vertexAndCommunityList: RDD[(Long, Set[Long])] =
      AssignCommunitiesToVertex.run(cliquesAndConnectedComponent.connectedComponents, cliquesAndConnectedComponent.cliqueGraphNewIDsToVerticesList)

    /**
     * TODO: Update the properties for each vertex by adding community property to it
     */
    //WriteBackToTitan(vertexAndCommunities)

  }

}
