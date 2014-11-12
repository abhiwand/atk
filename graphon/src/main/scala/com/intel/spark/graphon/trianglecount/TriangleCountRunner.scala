
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

package com.intel.spark.graphon.trianglecount

import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.apache.spark.graphx.{ Graph, Edge => GraphXEdge }
import org.apache.spark.graphx.lib.{ TriangleCount => GraphXTriangleCount }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Arguments for the TriangleCountRunnerArgs
 * @param posteriorProperty Name of the property to which the posteriors will be written.
 * @param priorProperties List of prior properties to consider for graph computation
 */
case class TriangleCountRunnerArgs(posteriorProperty: String,
                                   priorProperties: Option[List[String]])

/**
 * Provides a method for running triangle count on a graph using graphx. The result is a new graph with the
 * posterior value placed as a vertex property.
 */
object TriangleCountRunner extends Serializable {

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

  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], args: TriangleCountRunnerArgs): (RDD[GBVertex], RDD[GBEdge]) = {

    val outputPropertyLabel = args.posteriorProperty
    val inputEdgeLabels = args.priorProperties

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

    // run graphx trianglecount implementation
    val newGraph: Graph[Int, Long] = GraphXTriangleCount.run(graph)

    // extract vertices and edges from graphx graph instance
    val intermediateVertices: RDD[(Long, Property)] = newGraph.vertices.map({
      case (physicalId, triangleCount) => (physicalId, Property(outputPropertyLabel, triangleCount))
    })

    // Join the intermediate vertex/edge rdds with input vertex/edge rdd's to append the triangleCount attribute
    val outVertices: RDD[GBVertex] = inVertices
      .map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], gbVertex))
      .join(intermediateVertices)
      .map(vertex => generateGBVertex(vertex))

    (outVertices, inEdges)
  }

  // converts GBEdge to a GraphXEdge to be consumed by the triangle count computation
  // Note: GraphX Triangle count expects the edge to be in canonical orientation i.e. srcId < destId
  // Triangle Count makes sense on a undirected graph, hence we can change the edges to the canonical orientation
  // before passing it to graphx. We do not need the edges henceforth for join purposes.
  // Refer: https://spark.apache.org/docs/1.0.2/api/scala/index.html#org.apache.spark.graphx.lib.TriangleCount$
  private def createGraphXEdgeFromGBEdge(gbEdge: GBEdge, canonicalOrientation: Boolean = true): GraphXEdge[Long] = {
    val srcId = gbEdge.tailPhysicalId.asInstanceOf[Long]
    val destId = gbEdge.headPhysicalId.asInstanceOf[Long]
    if (canonicalOrientation && srcId > destId)
      GraphXEdge[Long](destId, srcId)
    else
      GraphXEdge[Long](srcId, destId)
  }

  // generates GBVertex from value pair obtained as a result of join and appends the pagerank property to the GBVertex
  private def generateGBVertex(joinValuePair: (Long, GBVertexPropertyPair)): GBVertex = {
    val (gbVertex, pagerankProperty) = joinValuePair._2 match {
      case value: GBVertexPropertyPair => (value._1, value._2)
    }
    gbVertex.copy(properties = gbVertex.properties + pagerankProperty)
  }

}
