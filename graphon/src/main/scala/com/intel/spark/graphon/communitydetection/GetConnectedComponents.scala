package com.intel.spark.graphon.communitydetection

import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.CreateGraphFromEnumeratedKCliques._
import com.intel.spark.graphon.idassigner._
import com.intel.spark.graphon.connectedcomponents.ConnectedComponentsGraphXDefault
import org.apache.spark.SparkContext

/**
 * Assign new Long IDs for each K-cliques of the k-clique graphs. Create a new graph using these Long IDs as
 * new vertices and run connected components to get communities
 */

object GetConnectedComponents {

  /**
   * Run the connected components and get the community IDs along with mapping between new Long IDs and original k-cliques
   * @param kCliqueGraphOutput KCliqueGraphOutput with set of k-cliques as vertices of new graph and
   *                           pair of k-cliques having (k-1) vertices common as edges of new graph
   * @param sparkContext SparkContext
   * @return connectedComponentsOutput
   */
  def run(kCliqueGraphOutput: KCliqueGraphOutput, sparkContext: SparkContext) = {

    val cliqueGraphVertices = kCliqueGraphOutput.cliqueGraphVertices
    val cliqueGraphEdges = kCliqueGraphOutput.cliqueGraphEdges

    //    Generate new Long IDs for each K-Clique in k-clique graph. These long IDs will be the vertices
    //    of a new graph. In this new graph, the edge between two vertices will exists if the two original
    //    k-cliques corresponding to the two vertices have exactly (k-1) number of elements in common
    val graphIDAssigner = new GraphIDAssigner[VertexSet](sparkContext)
    val graphIDAssignerOutput = graphIDAssigner.run(cliqueGraphVertices, cliqueGraphEdges)

    val newVerticesOfCliqueGraph = graphIDAssignerOutput.vertices
    val newEdgesOfCliqueGraph = graphIDAssignerOutput.edges
    val newVertexIdToOldVertexIdOfCliqueGraph: RDD[(Long, VertexSet)] = graphIDAssignerOutput.newIdsToOld

    val connectedComponents = ConnectedComponentsGraphXDefault.run(newVerticesOfCliqueGraph, newEdgesOfCliqueGraph)

    new ConnectedComponentsOutput(connectedComponents, newVertexIdToOldVertexIdOfCliqueGraph)

  }

  /**
   * Return value of connected components including the community IDs
   * @param connectedComponents pair of new vertex ID and community ID
   * @param newVertexIdToOldVertexIdOfCliqueGraph mapping between new vertex ID and original k-cliques
   */
  case class ConnectedComponentsOutput(connectedComponents: RDD[(Long, Long)], newVertexIdToOldVertexIdOfCliqueGraph: RDD[(Long, VertexSet)])

}
