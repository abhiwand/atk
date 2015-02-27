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

package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.elements.{ GBEdge, GraphElement, Property, GBVertex }
import com.thinkaurelius.titan.core.{ TitanProperty, TitanVertex }
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
  def createGbProperties(properties: Iterable[TitanProperty]): Set[Property] = {
    properties.map(p => Property(p.getPropertyKey().getName(), p.getValue())).toSet
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
        case v: GBVertex => {
          new GBVertex(v.physicalId, v.gbId, v.properties).asInstanceOf[GraphElement]
        }
        case e: GBEdge => {
          new GBEdge(None, e.tailPhysicalId, e.headPhysicalId, e.tailVertexGbId, e.headVertexGbId, e.label, e.properties).asInstanceOf[GraphElement]
        }
      }
    })
  }

  def createFaunusVertex(titanVertex: TitanVertex): FaunusVertex = {
    val faunusVertex = new FaunusVertex()
    faunusVertex.setId(titanVertex.getLongId)

    titanVertex.getProperties().map(property => {
      faunusVertex.addProperty(property.getPropertyKey().getName(), property.getValue())
    })

    titanVertex.getTitanEdges(Direction.OUT).map(edge => {
      val faunusEdge = faunusVertex.addEdge(edge.getLabel(), edge.getOtherVertex(titanVertex))
      edge.getPropertyKeys().map(property => faunusEdge.setProperty(property, edge.getProperty(property)))
    })
    faunusVertex
  }
}
