//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.thinkaurelius.titan.core.TitanGraph
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import com.tinkerpop.blueprints.Graph
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
    new StandardTitanGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(config)))
  }

}

object TitanGraphConnector {

  /**
   * Get graph name from Titan configuration based on the storage backend.
   *
   * Titan uses different options for specifying the graph name based on the backend.
   * For example, "storage.hbase.table" for HBase, and "storage.cassandra.keyspace" for Cassandra.
   *
   * @param titanConfig Titan configuration
   * @return Graph name
   */
  def getTitanGraphName(titanConfig: SerializableBaseConfiguration): String = {
    val storageBackend = titanConfig.getString("storage.backend")
    val graphNameKey = getTitanGraphNameKey(storageBackend)
    titanConfig.getString(graphNameKey)
  }

  /**
   * Set graph name in Titan configuration based on the storage backend.
   *
   * Titan uses different options for specifying the graph name based on the backend. For example,
   * "storage.hbase.table" for HBase, and "storage.cassandra.keyspace" for Cassandra.
   *
   * @param titanConfig Titan configuration
   * @param graphName Graph name
   */
  def setTitanGraphName(titanConfig: SerializableBaseConfiguration, graphName: String): Unit = {
    val storageBackend = titanConfig.getString("storage.backend")
    val graphNameKey = getTitanGraphNameKey(storageBackend)
    titanConfig.setProperty(graphNameKey, graphName)
  }

  /**
   * Get graph name configuration key based on the storage backend.
   *
   * Titan uses different options for specifying the graph name based on the backend. For example,
   * "storage.hbase.table" for HBase, and "storage.cassandra.keyspace" for Cassandra.
   *
   * @param storageBackend Name of Titan storage backend. For example (hbase, or cassandra)
   * @return Graph name configuration key based on the storage backend
   */
  def getTitanGraphNameKey(storageBackend: String): String = {
    storageBackend.toLowerCase match {
      case "hbase" => "storage.hbase.table"
      case "cassandra" => "storage.cassandra.keyspace"
      case _ => throw new RuntimeException("Unsupported storage backend for Titan. Please set storage.backend to hbase or cassandra")
    }
  }

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
}
