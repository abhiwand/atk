////////////////////////////////////////////////////////////////////////////////
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
////////////////////////////////////////////////////////////////////////////////

package com.intel.spark.graphon.pagerank

import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.apache.spark.graphx.{ Edge => GraphXEdge, PartitionStrategy, Graph }
import org.apache.spark.graphx.lib.{ PageRank => GraphXPageRank }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * Arguments for the PageRankRunnerArgs
 * @param posteriorProperty Name of the property to which the posteriors will be written.
 * @param priorProperties List of prior properties to consider for graph computation
 * @param maxIterations Maximum number of iterations to execute.
 * @param resetProbability Random reset probability
 * @param convergenceTolerance Tolerance allowed at convergence (smaller values tend to yield accurate results)
 */
case class PageRankRunnerArgs(posteriorProperty: String,
                              priorProperties: Option[List[String]],
                              maxIterations: Option[Int],
                              resetProbability: Option[Double],
                              convergenceTolerance: Option[Double])

/**
 * Provides a method for running pagerank on a graph using graphx. The result is a new graph with the pagerank
 * posterior placed in a new vertex and edge property on each vertex and edge respectively.
 */
object PageRankRunner extends Serializable {

  type EdgeSrcDestPair = (Long, Long)
  type GBVertexPropertyPair = (GBVertex, Property)
  type GBEdgePropertyPair = (GBEdge, Property)

  /**
   * Run pagerank on a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param args Parameters controlling the execution of pagerank.
   * @return Vertices and edges for the output graph.
   */

  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], args: PageRankRunnerArgs): (RDD[GBVertex], RDD[GBEdge]) = {

    val outputPropertyLabel = args.posteriorProperty
    val inputEdgeLabels = args.priorProperties
    val maxIterations: Int = args.maxIterations.getOrElse(PageRankDefaults.maxIterationsDefault)
    val resetProbability: Double = args.resetProbability.getOrElse(PageRankDefaults.resetProbabilityDefault)
    val convergenceTolerance: Double = args.convergenceTolerance.getOrElse(PageRankDefaults.convergenceToleranceDefault)

    // Only select edges as specified in inputEdgeLabels
    val filteredEdges: RDD[GBEdge] = inputEdgeLabels match {
      case None => inEdges
      case _ => inEdges.filter(edge => inputEdgeLabels.get.contains(edge.label))
    }

    // convert to graphX vertices
    val graphXVertices: RDD[(Long, Null)] =
      inVertices.map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], null))

    val graphXEdges: RDD[GraphXEdge[Long]] = filteredEdges.map(edge => createGraphXEdgeFromGBEdge(edge))

    // create graphx Graph instance from graphx vertices and edges
    val graph = Graph[Null, Long](graphXVertices, graphXEdges)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // run graphx pagerank implementation
    val newGraph: Graph[Double, Double] = args.convergenceTolerance match {
      case None => GraphXPageRank.run(graph, maxIterations, resetProbability)
      case _ => GraphXPageRank.runUntilConvergence(graph, convergenceTolerance, resetProbability)
    }

    // extract vertices and edges from graphx graph instance
    val intermediateVertices: RDD[(Long, Property)] = newGraph.vertices.map({
      case (physicalId, pageRank) => (physicalId, Property(outputPropertyLabel, pageRank))
    })

    val intermediateEdges: RDD[(EdgeSrcDestPair, Property)] =
      newGraph.edges.map(edge => ((edge.srcId, edge.dstId), Property(outputPropertyLabel, edge.attr)))

    // Join the intermediate vertex/edge rdds with input vertex/edge rdd's to append the pagerank attribute
    val outVertices: RDD[GBVertex] = inVertices
      .map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], gbVertex))
      .join(intermediateVertices)
      .map(vertex => generateGBVertex(vertex))

    val outEdges: RDD[GBEdge] = inEdges
      .map(gbEdge => ((gbEdge.tailPhysicalId.asInstanceOf[Long], gbEdge.headPhysicalId.asInstanceOf[Long]), gbEdge))
      .join(intermediateEdges)
      .map(edge => generateGBEdge(edge))

    graph.unpersistVertices()

    (outVertices, outEdges)
  }

  // converts GBEdge to a GraphXEdge to be consumed by the pagerank computation
  private def createGraphXEdgeFromGBEdge(gbEdge: GBEdge): GraphXEdge[Long] = {
    GraphXEdge[Long](gbEdge.tailPhysicalId.asInstanceOf[Long], gbEdge.headPhysicalId.asInstanceOf[Long])
  }

  // generates GBEdge from value pair obtained as a result of join and appends the pagerank property to the GBEdge
  private def generateGBEdge(joinValuePair: (EdgeSrcDestPair, GBEdgePropertyPair)): GBEdge = {
    val (gbEdge, pagerankProperty) = joinValuePair._2 match {
      case value: GBEdgePropertyPair => (value._1, value._2)
    }
    gbEdge.copy(properties = gbEdge.properties + pagerankProperty)
  }

  // generates GBVertex from value pair obtained as a result of join and appends the pagerank property to the GBVertex
  private def generateGBVertex(joinValuePair: (Long, GBVertexPropertyPair)): GBVertex = {
    val (gbVertex, pagerankProperty) = joinValuePair._2 match {
      case value: GBVertexPropertyPair => (value._1, value._2)
    }
    gbVertex.copy(properties = gbVertex.properties + pagerankProperty)
  }

}
