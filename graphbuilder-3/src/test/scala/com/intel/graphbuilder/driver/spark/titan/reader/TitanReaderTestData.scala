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

import java.io.File

import com.intel.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.testutils.DirectoryUtils
import com.thinkaurelius.titan.core.TitanVertex
import org.apache.hadoop.io.NullWritable
import org.scalatest.{ BeforeAndAfterAll, Suite }

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
 */
object TitanReaderTestData extends Suite with BeforeAndAfterAll {

  import com.intel.graphbuilder.driver.spark.titan.reader.TitanReaderUtils._

  val gbID = TitanReader.TITAN_READER_DEFAULT_GB_ID
  private var tmpDir: File = DirectoryUtils.createTempDirectory("titan-graph-for-unit-testing-")

  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.backend", "berkeleyje")
  titanConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

  val titanConnector = new TitanGraphConnector(titanConfig)
  val graph = titanConnector.connect()

  // Create a test graph which is a subgraph of Titan's graph of the gods
  val graphManager = graph.getManagementSystem()
  graphManager.makeEdgeLabel("brother").make()
  graphManager.makeEdgeLabel("lives").make()

  // Ordering properties alphabetically to ensure to that tests pass
  // Since properties are represented as a sequence, graph elements with different property orders are not considered equal
  graphManager.makePropertyKey("age").dataType(classOf[Integer]).make()
  graphManager.makePropertyKey("name").dataType(classOf[String]).make()
  graphManager.makePropertyKey("reason").dataType(classOf[String]).make()
  graphManager.makePropertyKey("type").dataType(classOf[String]).make()
  graphManager.commit()

  // Titan graph elements
  val neptuneTitanVertex = {
    val vertex = graph.addVertex(null)
    vertex.setProperty("age", 4500)
    vertex.setProperty("name", "neptune")
    vertex.setProperty("type", "god")
    vertex.asInstanceOf[TitanVertex]
  }

  val seaTitanVertex = {
    val vertex = graph.addVertex(null)
    vertex.setProperty("name", "sea")
    vertex.setProperty("type", "location")
    vertex.asInstanceOf[TitanVertex]
  }

  val plutoTitanVertex = {
    val vertex = graph.addVertex(null)
    vertex.setProperty("age", 4000)
    vertex.setProperty("name", "pluto")
    vertex.setProperty("type", "god")
    vertex.asInstanceOf[TitanVertex]
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
    new GBVertex(neptuneTitanVertex.getId, Property(gbID, neptuneTitanVertex.getId), gbNeptuneProperties)
  }

  val seaGbVertex = {
    val gbSeaProperties = createGbProperties(seaTitanVertex.getProperties())
    new GBVertex(seaTitanVertex.getId, Property(gbID, seaTitanVertex.getId), gbSeaProperties)
  }

  val plutoGbVertex = {
    val gbPlutoProperties = createGbProperties(plutoTitanVertex.getProperties())
    new GBVertex(plutoTitanVertex.getId, Property(gbID, plutoTitanVertex.getId), gbPlutoProperties)
  }

  val seaGbEdge = {
    val gbSeaEdgeProperties = Set(Property("reason", "loves waves"))
    new GBEdge(None, neptuneTitanVertex.getId, seaTitanVertex.getId, Property(gbID, neptuneTitanVertex.getId), Property(gbID, seaTitanVertex.getId), seaTitanEdge.getLabel(), gbSeaEdgeProperties)
  }

  val plutoGbEdge = {
    new GBEdge(None, neptuneTitanVertex.getId, plutoTitanVertex.getId, Property(gbID, neptuneTitanVertex.getId), Property(gbID, plutoTitanVertex.getId), plutoTitanEdge.getLabel(), Set[Property]())
  }

  // Faunus graph elements
  val neptuneFaunusVertex = createFaunusVertex(neptuneTitanVertex)
  val plutoFaunusVertex = createFaunusVertex(plutoTitanVertex)
  val seaFaunusVertex = createFaunusVertex(seaTitanVertex)

  // HBase rows
  val hBaseRows = Seq((NullWritable.get(), neptuneFaunusVertex)) //, (NullWritable.get(), plutoFaunusVertex), (NullWritable.get(), seaFaunusVertex))

  /**
   * IMPORTANT! removes temporary files
   */
  def cleanupTitan(): Unit = {
    try {
      if (graph != null) {
        graph.shutdown()
      }
    }
    finally {
      DirectoryUtils.deleteTempDirectory(tmpDir)
    }
  }

}
