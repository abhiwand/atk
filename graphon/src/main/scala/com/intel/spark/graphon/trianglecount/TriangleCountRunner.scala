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

package com.intel.spark.graphon.trianglecount

import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import com.intel.spark.graphon.graphconversions.GraphConversions
import org.apache.spark.graphx.{ Edge => GraphXEdge, PartitionStrategy, Graph }
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

    val graphXEdges: RDD[GraphXEdge[Long]] = filteredEdges.map(edge => GraphConversions.createGraphXEdgeFromGBEdge(edge, true)).distinct()

    // create graphx Graph instance from graphx vertices and edges
    val graph = Graph[Null, Long](graphXVertices, graphXEdges)
      .partitionBy(PartitionStrategy.RandomVertexCut)

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
      .map({ case (_, (vertex, property)) => vertex.copy(properties = vertex.properties + property) })

    (outVertices, inEdges)
  }
}
