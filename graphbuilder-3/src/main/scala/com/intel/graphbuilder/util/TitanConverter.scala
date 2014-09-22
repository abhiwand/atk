package com.intel.graphbuilder.util

import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.elements.{ Edge, Property, Vertex }
import com.thinkaurelius.titan.core.{ TitanVertex, TitanEdge, TitanProperty }
import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.tinkerpop.blueprints.Direction

import scala.collection.JavaConversions._

/**
 * Converts Titan graph elements to GraphBuilder graph elements, and vice versa.
 */
object TitanConverter {

  /**
   * Converts a Faunus (Titan-Hadoop) vertex into a Graph builder vertex
   *
   * @param faunusVertex Faunus Vertex
   * @param gbIdPropertyName Property name of unique ID used by graph builder
   * @return GraphBuilder vertex
   */
  def createGraphBuilderVertex(faunusVertex: FaunusVertex, gbIdPropertyName: Option[String] = None): Vertex = {
    val physicalId = faunusVertex.getId

    val gbId = getGbId(faunusVertex, gbIdPropertyName)
    val gbProperties = createGraphBuilderProperties(faunusVertex.getProperties)

    Vertex(physicalId, gbId, gbProperties)
  }

  /**
   * Converts Titan properties to Graph builder properties
   *
   * @param titanProperties Iterable of titan properties
   * @return Sequence of Graph builder properties
   */
  def createGraphBuilderProperties(titanProperties: Iterable[TitanProperty]): Seq[Property] = {
    val gbProperties = titanProperties.map(titanProperty =>
      Property(titanProperty.getPropertyKey.getName, titanProperty.getValue))
    gbProperties.toSeq
  }

  /**
   * Gets edges from a Faunus vertex, and converts the edges to Graph builder edges
   *
   * @param faunusVertex Faunus vertex
   * @param gbIdPropertyName Property name of unique ID used by graph builder
   * @return Iterator of Graph builder edges
   */
  def createGraphBuilderEdges(faunusVertex: FaunusVertex, gbIdPropertyName: Option[String] = None): Iterator[Edge] = {
    val titanEdges = faunusVertex.getTitanEdges(Direction.OUT).iterator()
    titanEdges.map(titanEdge => createGraphBuilderEdge(titanEdge, gbIdPropertyName))
  }

  /**
   * Converts a Titan Edge into a Graph builder edge
   *
   * @param titanEdge Titan edge
   * @param gbIdPropertyName Property name of unique ID used by graph builder
   * @return Graph builder edge
   */
  def createGraphBuilderEdge(titanEdge: TitanEdge, gbIdPropertyName: Option[String] = None): Edge = {
    val titanFromVertex = titanEdge.getVertex(Direction.OUT)
    val titanToVertex = titanEdge.getVertex(Direction.IN)
    val fromGbId = getGbId(titanFromVertex, gbIdPropertyName)
    val toGbId = getGbId(titanToVertex, gbIdPropertyName)

    val edgeProperties = titanEdge.getPropertyKeys.map(key => Property(key, titanEdge.getProperty(key))).toSeq

    Edge(titanFromVertex.getId, titanToVertex.getId, fromGbId, toGbId, titanEdge.getLabel, edgeProperties)
  }

  /**
   * Gets the Graph builder ID from a Titan Vertex
   *
   * @param titanVertex Titan Vertex
   * @param gbIdPropertyName Property name of unique ID used by graph builder
   * @return GraphBuilder property with Graph builder ID
   */
  def getGbId(titanVertex: TitanVertex, gbIdPropertyName: Option[String] = None): Property = {
    gbIdPropertyName match {
      case Some(propertyName) => Property(propertyName, titanVertex.getProperty(propertyName))
      case None => Property(TitanReader.TITAN_READER_DEFAULT_GB_ID, titanVertex.getId)
    }
  }
}


