package com.intel.spark.graphon.hierarchicalclustering

import java.io.Serializable

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.schema.GraphSchema
import com.intel.graphbuilder.schema.{ PropertyType, PropertyDef, EdgeLabelDef, GraphSchema }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.write.titan.TitanSchemaWriter
import com.intel.intelanalytics.domain.schema.GraphSchema
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{ Edge, Vertex }

object TitanStorage extends Serializable {

  def connectToTitan(titanConfig: SerializableBaseConfiguration): TitanGraph = {
    val titanConnector = new TitanGraphConnector(titanConfig)
    titanConnector.connect()
  }

  def addSchemaToTitan(titanGraph: TitanGraph): Unit = {

    val schema = new GraphSchema(List(EdgeLabelDef(HierarchicalClusteringConstants.LabelPropertyValue)),
      List(PropertyDef(PropertyType.Vertex, GraphSchema.labelProperty, classOf[String]),
        PropertyDef(PropertyType.Vertex, HierarchicalClusteringConstants.VertexNodeCountProperty, classOf[Long]),
        PropertyDef(PropertyType.Vertex, HierarchicalClusteringConstants.VertexNodeNameProperty, classOf[String])))
    val schemaWriter = new TitanSchemaWriter(titanGraph)

    schemaWriter.write(schema)
  }

  def addVertexToTitan(nodeCount: Long,
                       name: String,
                       titanGraph: TitanGraph): Vertex = {

    val vertex = titanGraph.addVertex(null)
    vertex.setProperty(GraphSchema.labelProperty, HierarchicalClusteringConstants.LabelPropertyValue)
    vertex.setProperty(GraphSchema.labelProperty, nodeCount)

    //
    // TODO: this is testing only, remove later.
    //
    vertex.setProperty(HierarchicalClusteringConstants.VertexNodeNameProperty, name)

    vertex
  }

  def addEdgeToTitan(src: Vertex, dest: Vertex,
                     titanGraph: TitanGraph): Edge = {
    titanGraph.addEdge(null, src, dest, HierarchicalClusteringConstants.LabelPropertyValue)

  }

  /**
   * TODO: this is for testing purposes only. Remove later
   * @param edge
   * @return
   */
  def getInMemoryVertextName(edge: HierarchicalClusteringEdge): String = {

    edge.src.toString + "_" + edge.dest.toString
  }
}
