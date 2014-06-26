package com.intel.spark.graphon.communitydetection

import com.intel.spark.graphon.titanreader.TitanReader
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Edge => GBEdge, GraphElement }
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._

object KCliquePercolationDriver {

  def edgeListFromGBEdgeList(gbEdgeList: RDD[GBEdge]): RDD[Edge] = {

    gbEdgeList.filter(e => (e.tailPhysicalId.asInstanceOf[Long] < e.headPhysicalId.asInstanceOf[Long])).
      map(e => (KCliquePercolationDataTypes.Edge(e.tailPhysicalId.asInstanceOf[Long], e.headPhysicalId.asInstanceOf[Long])))

  }

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
     * Convert the graph builder edge list to the edge list that can will used in KClique Percolation
     */
    val edgeList: RDD[Edge] = edgeListFromGBEdgeList(gbEdgeList)

    /**
     * Get all enumerated K-Cliques using the edge list
     */
    val enumeratedKCliques: RDD[ExtendersFact] = KCliqueEnumeration.applyToEdgeList(edgeList, k)

    /**
     * Construct the clique graph that will be input for connected components
     */
    val kcliqueGraphForComponentAnalysis = CreateGraphFromEnumeratedKCliques.applyToExtendersFact(enumeratedKCliques)

    /**
     * TODO: Call connected component analysis to get the communities
     */
    //val cliquesAndConnectedComponent = GetConnectedComponentsFrom(kcliqueGraphForComponentAnalysis)

    /**
     * TODO: Associate each vertex with a list of the communities to which it belongs
     */
    //val vertexAndCommunities = AssignCommunitiesToVertexUsing(cliquesAndConnectedComponent)

    /**
     * TODO: Update the properties for each vertex by adding community property to it
     */
    //WriteBackToTitan(vertexAndCommunities)

  }

}
