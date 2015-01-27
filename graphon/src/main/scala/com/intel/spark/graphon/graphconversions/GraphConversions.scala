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
package com.intel.spark.graphon.graphconversions

import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.{ Edge => GraphXEdge, PartitionStrategy, Graph }

object GraphConversions {

  type EdgeSrcDestPair = (Long, Long)
  type GBVertexPropertyPair = (GBVertex, Property)
  type GBEdgePropertyPair = (GBEdge, Property)

  /**
   * Converts GraphBuilder edges (ATK internal representation) into GraphX edges.
   *
   * @param gbEdge Incoming ATK edge to be converted into a graphx edge.
   * @param canonicalOrientation If true, edges are placed in a canonical orientation in which src < dst.
   * @return GraphX representation of the incoming edge.
   */
  def createGraphXEdgeFromGBEdge(gbEdge: GBEdge, canonicalOrientation: Boolean = false): GraphXEdge[Long] = {
    val srcId = gbEdge.tailPhysicalId.asInstanceOf[Long]
    val destId = gbEdge.headPhysicalId.asInstanceOf[Long]
    if (canonicalOrientation && srcId > destId)
      GraphXEdge[Long](destId, srcId)
    else
      GraphXEdge[Long](srcId, destId)
  }

  // generates GBVertex from value pair obtained as a result of join and appends the clustering coefficient
  // property to the GBVertex

  /**
   * Creates a new vertex from an incoming vertex and a property/value pair.
   * @param vertexPropertyPair The vertex property pair
   * @return A new vertex which is the result of adding the property to the incoming vertex.
   */
  def generateGBVertex(vertexPropertyPair: (Long, GBVertexPropertyPair)): GBVertex = {
    val (gbVertex, newProperty) = vertexPropertyPair._2 match {
      case value: GBVertexPropertyPair => (value._1, value._2)
    }
    gbVertex.copy(properties = gbVertex.properties + newProperty)
  }
}
