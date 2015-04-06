package com.intel.spark.graphon.hierarchicalclustering

import java.io.Serializable

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.schema.{ PropertyType, PropertyDef, EdgeLabelDef, GraphSchema }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.write.titan.TitanSchemaWriter
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{ Edge, Vertex }

object TitanStorage extends Serializable {

  def connectToTitan(titanConfig: SerializableBaseConfiguration): TitanGraph = {
    val titanConnector = new TitanGraphConnector(titanConfig)
    titanConnector.connect()
  }

  def addSchemaToTitan(titanGraph: TitanGraph): Unit = {

    val schema = new GraphSchema(List(EdgeLabelDef(HierarchicalClusteringConstants.LabelProperty)),
      List(PropertyDef(PropertyType.Vertex, HierarchicalClusteringConstants.VertexLabelProperty, classOf[String]),
        PropertyDef(PropertyType.Vertex, HierarchicalClusteringConstants.VertexNodeCountProperty, classOf[Int]),
        PropertyDef(PropertyType.Vertex, HierarchicalClusteringConstants.VertexNodeNameProperty, classOf[String])))
    val schemaWriter = new TitanSchemaWriter(titanGraph)

    schemaWriter.write(schema)
  }

  def addVertexToTitan(nodeCount: Int,
                       name: String,
                       titanGraph: TitanGraph): Vertex = {

    val vertex = titanGraph.addVertex(null)
    vertex.setProperty(HierarchicalClusteringConstants.VertexLabelProperty, HierarchicalClusteringConstants.LabelProperty)
    vertex.setProperty(HierarchicalClusteringConstants.VertexNodeCountProperty, nodeCount)

    //
    // TODO: this is testing only, remove later.
    //
    vertex.setProperty(HierarchicalClusteringConstants.VertexNodeNameProperty, name)

    vertex
  }

  def addEdgeToTitan(src: Vertex, dest: Vertex,
                     titanGraph: TitanGraph): Edge = {
    titanGraph.addEdge(null, src, dest, HierarchicalClusteringConstants.LabelProperty)

  }

  def commit(titanGraph: TitanGraph): Unit = {

    titanGraph.commit()
  }

  def shutDown(titanGraph: TitanGraph): Unit = {

    titanGraph.shutdown()
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
