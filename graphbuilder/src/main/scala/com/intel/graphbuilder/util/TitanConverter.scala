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

package com.intel.graphbuilder.util

import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.elements.{ GBEdge, Property, GBVertex }
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
  def createGraphBuilderVertex(faunusVertex: FaunusVertex, gbIdPropertyName: Option[String] = None): GBVertex = {
    val physicalId = faunusVertex.getId

    val gbId = getGbId(faunusVertex, gbIdPropertyName)
    val gbProperties = createGraphBuilderProperties(faunusVertex.getProperties)

    GBVertex(physicalId, gbId, gbProperties)
  }

  /**
   * Converts Titan properties to Graph builder properties
   *
   * @param titanProperties Iterable of titan properties
   * @return Sequence of Graph builder properties
   */
  def createGraphBuilderProperties(titanProperties: Iterable[TitanProperty]): Set[Property] = {
    val gbProperties = titanProperties.map(titanProperty =>
      Property(titanProperty.getPropertyKey.getName, titanProperty.getValue))
    gbProperties.toSet
  }

  /**
   * Gets edges from a Faunus vertex, and converts the edges to Graph builder edges
   *
   * @param faunusVertex Faunus vertex
   * @param gbIdPropertyName Property name of unique ID used by graph builder
   * @return Iterator of Graph builder edges
   */
  def createGraphBuilderEdges(faunusVertex: FaunusVertex, gbIdPropertyName: Option[String] = None): Iterator[GBEdge] = {
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
  def createGraphBuilderEdge(titanEdge: TitanEdge, gbIdPropertyName: Option[String] = None): GBEdge = {
    val titanFromVertex = titanEdge.getVertex(Direction.OUT)
    val titanToVertex = titanEdge.getVertex(Direction.IN)
    val fromGbId = getGbId(titanFromVertex, gbIdPropertyName)
    val toGbId = getGbId(titanToVertex, gbIdPropertyName)
    val eid = titanEdge.getLongId
    val edgeProperties = titanEdge.getPropertyKeys.map(key => Property(key, titanEdge.getProperty(key))).toSet

    GBEdge(Some(eid), titanFromVertex.getId, titanToVertex.getId, fromGbId, toGbId, titanEdge.getLabel, edgeProperties)
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
