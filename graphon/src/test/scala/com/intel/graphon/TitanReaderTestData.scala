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

package com.intel.graphon

import java.io.File

import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.elements.{Edge, Property, Vertex}
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.testutils.DirectoryUtils
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConversions._

/**
 * A collection of data used to test reading from a Titan graph.
 *
 * The test data represents a subgraph of Titan's graph of the god's example in multiple formats namely as:
 * 1. Titan graph elements
 * 2. GraphBuilder graph elements
 * 3. Serialized Titan rows, where each row represents a vertex and its adjacency list
 * 4. Serialized HBase rows, where each row represents a vertex and its adjacency list
 *
 * @todo Use Stephen's TestingTitan class for scalatest
 */
object TitanReaderTestData extends Suite with BeforeAndAfterAll {

  import com.intel.graphon.TitanReaderUtils._

  val gbID = TitanReader.TITAN_READER_DEFAULT_GB_ID
  private var tmpDir: File = DirectoryUtils.createTempDirectory("titan-graph-for-unit-testing-")

  var titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

  var titanConnector = new TitanGraphConnector(titanConfig)
  var graph = titanConnector.connect()
  val transaction = graph.newTransaction(graph.buildTransaction())

  // Create a test graph which is a subgraph of Titan's graph of the gods
  graph.makeLabel("brother").make()
  graph.makeLabel("lives").make()

  // Ordering properties alphabetically to ensure to that tests pass
  // Since properties are represented as a sequence, graph elements with different property orders are not considered equal
  graph.makeKey("age").dataType(classOf[Integer]).make()
  graph.makeKey("name").dataType(classOf[String]).make()
  graph.makeKey("reason").dataType(classOf[String]).make()
  graph.makeKey("type").dataType(classOf[String]).make()
  graph.commit()

  // Titan graph elements
  val neptuneTitanVertex = {
    val vertex = graph.addVertex(null)
    vertex.setProperty("age", 4500)
    vertex.setProperty("name", "neptune")
    vertex.setProperty("type", "god")
    vertex
  }

  val seaTitanVertex = {
    val vertex = graph.addVertex(null)
    vertex.setProperty("name", "sea")
    vertex.setProperty("type", "location")
    vertex
  }

  val plutoTitanVertex = {
    val vertex = graph.addVertex(null)
    vertex.setProperty("age", 4000)
    vertex.setProperty("name", "pluto")
    vertex.setProperty("type", "god")
    vertex
  }

  val seaTitanEdge = {
    val edge = neptuneTitanVertex.addEdge("lives", seaTitanVertex)
    edge.setProperty("reason", "loves waves")
    edge
  }

  val plutoTitanEdge = neptuneTitanVertex.addEdge("brother", plutoTitanVertex)

  // GraphBuilder graph elements
  val neptuneGbVertex = {
    val gbNeptuneProperties = createGbProperties(neptuneTitanVertex.getProperties())
    new Vertex(neptuneTitanVertex.getID(), Property(gbID, neptuneTitanVertex.getID()), gbNeptuneProperties)
  }

  val seaGbVertex = {
    val gbSeaProperties = createGbProperties(seaTitanVertex.getProperties())
    new Vertex(seaTitanVertex.getID(), Property(gbID, seaTitanVertex.getID()), gbSeaProperties)
  }

  val plutoGbVertex = {
    val gbPlutoProperties = createGbProperties(plutoTitanVertex.getProperties())
    new Vertex(plutoTitanVertex.getID(), Property(gbID, plutoTitanVertex.getID()), gbPlutoProperties)
  }

  val seaGbEdge = {
    val gbSeaEdgeProperties = List(Property("reason", "loves waves"))
    new Edge(neptuneTitanVertex.getID, seaTitanVertex.getID, Property(gbID, neptuneTitanVertex.getID()),
      Property(gbID, seaTitanVertex.getID()), seaTitanEdge.getLabel(), gbSeaEdgeProperties)
  }

  val plutoGbEdge = {
    new Edge(neptuneTitanVertex.getID, plutoTitanVertex.getID, Property(gbID, neptuneTitanVertex.getID()),
      Property(gbID, plutoTitanVertex.getID()), plutoTitanEdge.getLabel(), List[Property]())
  }

  // Serialized Titan rows created using the Titan graph elements defined above.
  val titanRowMap = createTestTitanRows(graph)

  // Serialized HBase rows
  val hBaseRowMap = createTestHBaseRows(titanRowMap)

  override def afterAll = {
    cleanupTitan()
  }

  /**
   * IMPORTANT! removes temporary files
   */
  def cleanupTitan(): Unit = {
    try {
      if (graph != null) {
        transaction.commit()
        graph.shutdown()
      }
    } finally {
      DirectoryUtils.deleteTempDirectory(tmpDir)
    }

    // make sure this class is unusable when we're done
    titanConfig = null
    titanConnector = null
    graph = null
    tmpDir = null
  }

}

