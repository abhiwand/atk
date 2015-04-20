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
