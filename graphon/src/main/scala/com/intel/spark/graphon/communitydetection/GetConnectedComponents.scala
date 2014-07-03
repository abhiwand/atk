package com.intel.spark.graphon.communitydetection

import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.CreateGraphFromEnumeratedKCliques._
import com.intel.spark.graphon.idassigner._
import com.intel.spark.graphon.connectedcomponents.ConnectedComponentsGraphXDefault

/**
 * Assign new Long IDs for each K-cliques of the k-clique graphs. Create a new graph using these Long IDs as
 * new vertices and run connected components to get communities
 */

object GetConnectedComponents {

  /**
   * Generate new Long IDs for each K-Clique in k-clique graph. These long IDs will be the vertices
   * of a new graph. In this new graph, the edge between two vertices will exists if the two original
   * k-cliques corresponding to the two vertices have exactly (k-1) number of elements in common
   * @param kcliqueGraphForComponentAnalysis KCliqueGraphOutput having set of k-cliques and pair of kcliques having (k-1) vertices common
   * @return GraphIDAssignerOutput
   */
  def idAssignerForCliqueGraph(kcliqueGraphForComponentAnalysis: KCliqueGraphOutput) = {
    val cliqueFactEdges = kcliqueGraphForComponentAnalysis.cliqueFactEdgeList
    val cliqueFactVertices = kcliqueGraphForComponentAnalysis.cliqueFactVertexList

    val cliqueVertices = cliqueFactVertices.map({
      case (CliqueFact(cliqueVertex)) => cliqueVertex
    })
    val cliqueEdges: RDD[(KCliquePercolationDataTypes.VertexSet, KCliquePercolationDataTypes.VertexSet)] = cliqueFactEdges.map({
      case (CliqueFact(idCliqueVertex), CliqueFact(nbrCliqueVertex)) => (idCliqueVertex, nbrCliqueVertex)
    })

    val newGraphIDAssigner = new GraphIDAssigner[VertexSet](cliqueVertices.sparkContext)
    newGraphIDAssigner.run(cliqueVertices, cliqueEdges)

  }

  /**
   * Run the connected components and get the community IDs along with mapping between new Long IDs and original k-cliques
   * @param kcliqueGraphForComponentAnalysis KCliqueGraphOutput with set of k-cliques as vertices of new graph and
   *                                         pair of k-cliques having (k-1) vertices common as edges of new graph
   * @return connectedComponentsOutput
   */
  def run(kcliqueGraphForComponentAnalysis: KCliqueGraphOutput) = {

    val cliqueGraphNewAssignedID = idAssignerForCliqueGraph(kcliqueGraphForComponentAnalysis)

    val cliqueGraphAssignedVerticesID = cliqueGraphNewAssignedID.vertices
    val cliqueGraphAssignedEdgesID = cliqueGraphNewAssignedID.edges
    val cliqueGraphNewIDsToVerticesList: RDD[(Long, VertexSet)] = cliqueGraphNewAssignedID.newIdsToOld

    val connectedComponents = ConnectedComponentsGraphXDefault.run(cliqueGraphAssignedVerticesID, cliqueGraphAssignedEdgesID)

    new connectedComponentsOutput(connectedComponents, cliqueGraphNewIDsToVerticesList)

  }

  /**
   * Return value of connected components including the community IDs
   * @param connectedComponents pair of new vertex ID and community ID
   * @param cliqueGraphNewIDsToVerticesList mapping between new vertex ID and original k-cliques
   */
  case class connectedComponentsOutput(val connectedComponents: RDD[(Long, Long)], val cliqueGraphNewIDsToVerticesList: RDD[(Long, VertexSet)])

}
