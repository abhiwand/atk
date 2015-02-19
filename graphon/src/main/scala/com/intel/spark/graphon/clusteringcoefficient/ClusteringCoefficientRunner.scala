//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.spark.graphon.clusteringcoefficient

import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import com.intel.spark.graphon.graphconversions.GraphConversions
import org.apache.spark.graphx.lib.ia.plugins.ClusteringCoefficient
import org.apache.spark.graphx.{ Edge => GraphXEdge, PartitionStrategy, Graph }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Return value for the clustering coefficient runner
 * @param vertices RDD of vertices with the local clustering coefficient placed in the specified property.
 * @param globalClusteringCoefficient The global clustering coefficient of the input graph.
 */
case class ClusteringCoefficientRunnerReturn(vertices: RDD[GBVertex], globalClusteringCoefficient: Double)

/**
 * Provides a method for running clustering coefficient on a graph using graphx. The result is a new graph with the
 * local clustering coefficient placed as a vertex property.
 */
object ClusteringCoefficientRunner extends Serializable {

  /**
   * Run clustering coefficient analysis of a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param outputPropertyLabel Optional name of the vertex property for storing local clustering coefficients.
   * @param inputEdgeLabels Optional list of edge labels to consider for clustering coefficient computation.
   * @return Vertices and edges for the output graph.
   */

  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], outputPropertyLabel: Option[String], inputEdgeLabels: Option[Set[String]]): ClusteringCoefficientRunnerReturn = {

    // clustering coefficient is an undirected graph algorithm, so the input graph should
    // have the directed edge (b,a) present whenever the directed edge (a,b) is present... furthermore,
    // graphx expects one edge to be present ... from Min(a,b) to Max(a,b)
    val canonicalEdges: RDD[GBEdge] =
      inEdges.filter(gbEdge => gbEdge.tailPhysicalId.asInstanceOf[Long] < gbEdge.headPhysicalId.asInstanceOf[Long])

    val filteredEdges: RDD[GBEdge] = if (inputEdgeLabels.isEmpty) {
      canonicalEdges
    }
    else {
      canonicalEdges.filter(edge => inputEdgeLabels.get.contains(edge.label))
    }

    // convert to graphX vertices
    val graphXVertices: RDD[(Long, Null)] =
      inVertices.map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], null))

    val graphXEdges: RDD[GraphXEdge[Long]] = filteredEdges.map(edge => GraphConversions.createGraphXEdgeFromGBEdge(edge))

    // create graphx Graph instance from graphx vertices and edges
    val graph = Graph[Null, Long](graphXVertices, graphXEdges)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // run graphx clustering coefficient implementation

    val (newGraph, globalClusteringCoefficient) = ClusteringCoefficient.run(graph)

    val outVertices = if (outputPropertyLabel.nonEmpty) {
      val outputProperty = outputPropertyLabel.get
      // extract vertices and edges from graphx graph instance
      val intermediateVertices: RDD[(Long, Property)] = newGraph.vertices.map({
        case (physicalId, clusteringCoefficient) => (physicalId, Property(outputProperty, clusteringCoefficient))
      })

      // Join the intermediate vertex/edge rdds with input vertex/edge rdd's to append the triangleCount attribute
      inVertices
        .map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], gbVertex))
        .join(intermediateVertices)
        .map({ case (_, (vertex, property)) => vertex.copy(properties = vertex.properties + property) })
    }
    else {
      inVertices
    }

    ClusteringCoefficientRunnerReturn(outVertices, globalClusteringCoefficient)
  }

}