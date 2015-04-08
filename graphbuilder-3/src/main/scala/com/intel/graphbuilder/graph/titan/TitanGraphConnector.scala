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

package com.intel.graphbuilder.graph.titan

import java.io.File

import com.intel.graphbuilder.graph.GraphConnector
import com.thinkaurelius.titan.core.{ TitanFactory, TitanGraph }
import com.tinkerpop.blueprints.Graph
import com.intel.graphbuilder.titan.cache.TitanGraphCache

import org.apache.commons.configuration.{ Configuration, PropertiesConfiguration }

import scala.collection.JavaConversions._

/**
 * Get a connection to Titan.
 * <p>
 * This was needed because a 'Connector' can be serialized across the network, whereas a live connection can't.
 * </p>
 */
case class TitanGraphConnector(config: Configuration) extends GraphConnector with Serializable {

  /**
   * Initialize using a properties configuration file.
   * @param propertiesFile a .properties file, see example-titan.properties and Titan docs
   */
  def this(propertiesFile: File) {
    this(new PropertiesConfiguration(propertiesFile))
  }

  /**
   * Get a connection to a graph database.
   *
   * Returns a StandardTitanGraph which is a superset of TitanGraph. StandardTitanGraph implements additional
   * methods required to load graphs from Titan.
   */
  override def connect(): TitanGraph = {
    TitanFactory.open(config)
  }

}

object TitanGraphConnector {

  val titanGraphCache = new TitanGraphCache()

  /**
   * Helper method to resolve ambiguous reference error in TitanGraph.getVertices() in Titan 0.5.1+
   *
   * "Error:(96, 18) ambiguous reference to overloaded definition, both method getVertices in
   * trait TitanGraphTransaction of type (x$1: <repeated...>[Long])java.util.Map[Long,com.thinkaurelius.titan.core.TitanVertex]
   * and  method getVertices in trait Graph of type ()Iterable[com.tinkerpop.blueprints.Vertex]
   * match expected type ?
   */
  def getVertices(titanGraph: Graph): Iterable[com.tinkerpop.blueprints.Vertex] = {
    val vertices: Iterable[com.tinkerpop.blueprints.Vertex] = titanGraph.getVertices
    vertices
  }

  /**
   * Get Titan graph from cache
   *
   * @param titanConnector Titan connector
   * @return Titan graph
   */
  def getGraphFromCache(titanConnector: TitanGraphConnector): TitanGraph = {
    val titanGraph = titanGraphCache.getGraph(titanConnector.config)
    titanGraph
  }

  /**
   * Invalidate all entries in the cache when it is shut down
   */
  def invalidateGraphCache(): Unit = {
    titanGraphCache.invalidateAllCacheEntries
    System.out.println("Invalidating Titan graph cache:" + titanGraphCache)
  }
}
