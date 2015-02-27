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

package com.intel.spark.graphon.connectedcomponents

import com.intel.graphbuilder.elements.{ Property, GBVertex }
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{ Edge => GraphXEdge }
import org.apache.spark.graphx.lib.{ ConnectedComponents => GraphXConnectedComponents }
import org.apache.spark.SparkContext._

/**
 * Determine connected components of a graph. The input is a vertex list (an RDD of Longs) and an edge list
 * (an RDD of pairs of Longs) and the output is a list of (vertexId, componentID) pairs (an RDD of pairs of Longs).
 *
 * This code simply implements the Pegasus (aka Hash-Min) message passing
 * algorithm in GraphX.
 */

object ConnectedComponentsGraphXDefault {

  def run(vertexList: RDD[Long], edgeList: RDD[(Long, Long)]): RDD[(Long, Long)] = {

    val graphXVertices: RDD[(Long, Null)] = vertexList.map((vid: Long) => (vid, null))
    val graphXEdges: RDD[GraphXEdge[Null]] = edgeList.map(edge => (new GraphXEdge[Null](edge._1, edge._2, null)))

    val graph: Graph[Null, Null] = Graph(graphXVertices, graphXEdges)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    val outGraph = GraphXConnectedComponents.run(graph)

    val out: RDD[(Long, Long)] = outGraph.vertices.map({
      case (vertexId, connectedComponentId) => (vertexId, connectedComponentId.toLong)
    })

    out
  }

  type GBVertexPropertyPair = (GBVertex, Property)

  def mergeConnectedComponentResult(resultRDD: RDD[(Long, Property)], gbVertexRDD: RDD[GBVertex]): RDD[GBVertex] = {
    gbVertexRDD
      .map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], gbVertex))
      .join(resultRDD)
      .map(vertex => generateGBVertex(vertex))
  }

  // generates GBVertex from value pair obtained as a result of join and appends the pagerank property to the GBVertex
  private def generateGBVertex(joinValuePair: (Long, GBVertexPropertyPair)): GBVertex = {
    val (gbVertex, pagerankProperty) = joinValuePair._2 match {
      case value: GBVertexPropertyPair => (value._1, value._2)
    }
    gbVertex.copy(properties = gbVertex.properties + pagerankProperty)
  }

}
