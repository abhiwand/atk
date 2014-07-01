package com.intel.spark.graphon.communitydetection

import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.GetConnectedComponents._
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._
import org.apache.spark.SparkContext._

/**
 * Get the mapping pair between vertices of each k-cliques and community ID that has been generated from
 * connected components algorithm
 */

object AssignCommunitiesToVertex {

  /**
   * Emit pairs of vertex ID and community ID from k-clique graph vertex (having a new Long ID) which is originally a set of vertices.
   * Group by vertex IDs so that each vertex ID gets a list (possibly empty) of the community IDs to which it belongs
   *
   * @param connectedComponents pair of new Long IDs (corresponding to each k-cliques) and community ID
   * @param cliqueGraphNewIDsToVerticesList pair of new Long IDs and corresponding k-cliques
   * @return an RDD of pair of original vertex ID and list of community IDs to which it belongs
   */
  def run(connectedComponents: RDD[(Long, Long)], cliqueGraphNewIDsToVerticesList: RDD[(Long, VertexSet)]): RDD[(Long, Set[Long])] = {

    val communityCoGroupedWithVerticesList: RDD[(Seq[Long], Seq[Set[Long]])] = connectedComponents.cogroup(cliqueGraphNewIDsToVerticesList).map(_._2)
    val communityVerticesList: RDD[(Long, Set[Long])] = communityCoGroupedWithVerticesList.flatMap({ case (communityIdList, verticesList) => communityIdList.flatMap(communityID => verticesList.map(vertices => (communityID, vertices))) })
    val communityVertex: RDD[VertexCommunity] = communityVerticesList.flatMap(communityVertices => communityVertices._2.map(vertex => VertexCommunity(vertex, communityVertices._1)))

    val vertexCommunityList: RDD[(Long, Set[Long])] = communityVertex.groupBy(_.vertex).mapValues(_.map(_.communityID).toSet)
    vertexCommunityList
  }

}
