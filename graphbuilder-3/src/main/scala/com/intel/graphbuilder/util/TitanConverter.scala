package com.intel.graphbuilder.util

import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.elements.{ Edge, Property, Vertex }
import com.thinkaurelius.titan.core.{ TitanVertex, TitanEdge, TitanProperty }
import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.tinkerpop.blueprints.Direction

import scala.collection.JavaConversions._

object TitanConverter {

  def toGraphBuilderVertex(faunusVertex: FaunusVertex, gbIdPropertyName: Option[String] = None): Vertex = {
    val physicalId = faunusVertex.getId

    val gbId = getGbId(faunusVertex, gbIdPropertyName)
    val gbProperties = toGraphBuilderProperties(faunusVertex.getProperties)

    Vertex(physicalId, gbId, gbProperties)
  }

  def getGbId(titanVertex: TitanVertex, gbIdPropertyName: Option[String] = None): Property = {
    gbIdPropertyName match {
      case Some(propertyName) => Property(propertyName, titanVertex.getProperty(propertyName))
      case None => Property(TitanReader.TITAN_READER_DEFAULT_GB_ID, titanVertex.getId)
    }
  }

  def toGraphBuilderProperties(titanProperties: Iterable[TitanProperty]): Seq[Property] = {
    val gbProperties = titanProperties.map(titanProperty =>
      Property(titanProperty.getPropertyKey.getName, titanProperty.getValue))
    gbProperties.toSeq
  }

  def toGraphBuilderEdges(faunusVertex: FaunusVertex, gbIdPropertyName: Option[String] = None): Iterator[Edge] = {
    val titanEdges = faunusVertex.getEdges.iterator()

    titanEdges.map(titanEdge => toGraphBuilderEdge(titanEdge, gbIdPropertyName))
  }

  def toGraphBuilderEdge(titanEdge: TitanEdge, gbIdPropertyName: Option[String] = None): Edge = {
    val titanFromVertex = titanEdge.getVertex(Direction.IN)
    val titanToVertex = titanEdge.getVertex(Direction.OUT)
    val fromGbId = getGbId(titanFromVertex, gbIdPropertyName)
    val toGbId = getGbId(titanToVertex, gbIdPropertyName)

    val edgeProperties = titanEdge.getPropertyKeys.map(key => Property(key, titanEdge.getProperty(key))).toSeq

    Edge(titanFromVertex.getId, titanToVertex.getId, fromGbId, toGbId, titanEdge.getLabel, edgeProperties)
  }
}
