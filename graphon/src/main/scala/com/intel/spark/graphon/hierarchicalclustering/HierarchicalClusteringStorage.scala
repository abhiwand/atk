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

package com.intel.spark.graphon.hierarchicalclustering

import java.io.Serializable

import com.intel.graphbuilder.schema.{ PropertyType, PropertyDef, EdgeLabelDef, GraphSchema }
import com.intel.graphbuilder.write.titan.TitanSchemaWriter
import com.intel.intelanalytics.domain.schema.GraphSchema
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{ Edge, Vertex }

case class HierarchicalClusteringStorage(titanStorage: TitanGraph)
    extends HierarchicalClusteringStorageInterface {

  override def addSchema(): Unit = {

    val schema = new GraphSchema(
      List(EdgeLabelDef(HierarchicalClusteringConstants.LabelPropertyValue)),
      List(PropertyDef(PropertyType.Vertex, GraphSchema.labelProperty, classOf[String]),
        PropertyDef(PropertyType.Vertex, HierarchicalClusteringConstants.VertexNodeCountProperty, classOf[Long]),
        PropertyDef(PropertyType.Vertex, HierarchicalClusteringConstants.VertexNodeNameProperty, classOf[String]),
        PropertyDef(PropertyType.Vertex, HierarchicalClusteringConstants.VertexIterationProperty, classOf[Int]))
    )
    val schemaWriter = new TitanSchemaWriter(titanStorage)

    schemaWriter.write(schema)
  }

  override def addVertexAndEdges(src: Long, dest: Long, count: Long, name: String, iteration: Int): Long = {

    val metaNodeVertex = addVertex(count, name, iteration)
    addEdge(metaNodeVertex, src)
    addEdge(metaNodeVertex, dest)

    metaNodeVertex.getId.asInstanceOf[Long]
  }

  override def commit(): Unit = {
    titanStorage.commit()
  }

  override def shutdown(): Unit = {
    titanStorage.shutdown()
  }

  private def addVertex(vertexCount: Long, vertexName: String, iteration: Int): Vertex = {

    val vertex = titanStorage.addVertex(null)
    vertex.setProperty(GraphSchema.labelProperty, HierarchicalClusteringConstants.LabelPropertyValue)
    vertex.setProperty(GraphSchema.labelProperty, vertexCount)

    // TODO: this is testing only, remove later.
    vertex.setProperty(HierarchicalClusteringConstants.VertexNodeNameProperty, vertexName)
    vertex.setProperty(HierarchicalClusteringConstants.VertexIterationProperty, iteration)

    vertex
  }

  private def addEdge(src: Vertex, dest: Long): Edge = {
    titanStorage.addEdge(null, src, titanStorage.getVertex(dest), HierarchicalClusteringConstants.LabelPropertyValue)
  }

}
