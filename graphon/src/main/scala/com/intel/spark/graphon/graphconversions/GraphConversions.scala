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

  /**
   * Creates a new vertex from an incoming vertex by copying the vertex and
   * adding a given property to that vertex's property list.
   * @param property A property to add to the vertex.
   * @param vertex The vertex which will be copied and the property will be added to the copy.
   * @return A new vertex which is the result of adding the property to the incoming vertex.
   */
  def addPropertyToVertex(property: Property, vertex: GBVertex): GBVertex = {
    vertex.copy(properties = vertex.properties + property)
  }

  /**
   * Creates a new edge from an incoming edge by copying the edge and
   * adding a given property to that edge's property list.
   * @param property A property to add to the edge.
   * @param edge The edge which will be copied and the property will be added to the copy.
   * @return A new edge which is the result of adding the property to the incoming vertex.
   */
  def addPropertyToEdge(property: Property, edge: GBEdge): GBEdge = {
    edge.copy(properties = edge.properties + property)
  }
}
