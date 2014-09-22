package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.elements.{Edge, GraphElement, Property, Vertex}
import com.thinkaurelius.titan.core.{TitanProperty, TitanVertex}
import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.tinkerpop.blueprints.Direction

import scala.collection.JavaConversions._

/**
 * Utility methods for creating test data for reading Titan graphs.
 *
 * These utilities serialize Titan graph elements into the format that Titan uses for its key-value stores.
 * The utilities also create GraphBuilder elements from Titan elements.
 */
object TitanReaderUtils {

  /**
   * Create GraphBuilder properties from a list of Titan properties.
   *
   * @param properties Titan properties
   * @return Iterable of GraphBuilder properties
   */
  def createGbProperties(properties: Iterable[TitanProperty]): Seq[Property] = {
    properties.map(p => Property(p.getPropertyKey().getName(), p.getValue())).toList
  }

  /**
   * Orders properties in GraphBuilder elements alphabetically using the property key.
   *
   * Needed to ensure to that comparison tests pass. Graphbuilder properties are represented
   * as a sequence, so graph elements with different property orderings are not considered equal.
   *
   * @param graphElements Array of GraphBuilder elements
   * @return  Array of GraphBuilder elements with sorted property lists
   */
  def sortGraphElementProperties(graphElements: Array[GraphElement]) = {
    graphElements.map(element => {
      element match {
        case v: Vertex => {
          new Vertex(v.physicalId, v.gbId, v.properties.sortBy(p => p.key)).asInstanceOf[GraphElement]
        }
        case e: Edge => {
          new Edge(e.tailPhysicalId, e.headPhysicalId, e.tailVertexGbId, e.headVertexGbId, e.label, e.properties.sortBy(p => p.key)).asInstanceOf[GraphElement]
        }
      }
    })
  }

  def createFaunusVertex(titanVertex: TitanVertex): FaunusVertex = {
    val faunusVertex = new FaunusVertex()
    faunusVertex.setId(titanVertex.getLongId)

    titanVertex.getProperties().map(property => {
      faunusVertex.addProperty(property.getPropertyKey().getName(),property.getValue())
    })

    titanVertex.getTitanEdges(Direction.OUT).map(edge => {
      val faunusEdge = faunusVertex.addEdge(edge.getLabel(), edge.getOtherVertex(titanVertex))
      edge.getPropertyKeys().map (property => faunusEdge.setProperty(property, edge.getProperty(property)))
    })
    faunusVertex
  }
}
