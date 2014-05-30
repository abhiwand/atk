package com.intel.graphbuilder.testutils

import com.intel.graphbuilder.elements.Property
import scala.collection.JavaConversions._

import java.io.File
import com.intel.graphbuilder.testutils.DirectoryUtils._
import com.intel.graphbuilder.elements.Edge
import com.intel.graphbuilder.elements.Vertex
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader

/**
 * A collection of data used to test reading from a Titan graph.
 *
 * The test data represents a subgraph of Titan's graph of the god's example in multiple formats namely as:
 * 1. Titan graph elements
 * 2. GraphBuilder graph elements
 * 3. Serialized Titan rows, where each row represents a vertex and its adjacency list
 * 4. Serialized HBase rows, where each row represents a vertex and its adjacency list
 */
object TitanReaderTestData {

  import TitanReaderUtils._

  val gbID = TitanReader.TITAN_READER_GB_ID
  private var tmpDir: File = createTempDirectory("titan-graph-for-unit-testing-")

  var titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

  var titanConnector = new TitanGraphConnector(titanConfig)
  var graph = titanConnector.connect()

  // Create a test graph which is a subgraph of Titan's graph of the gods
  graph.makeLabel("brother").make()
  graph.makeLabel("lives").make()
  graph.makeKey("name").dataType(classOf[String]).make()
  graph.makeKey("type").dataType(classOf[String]).make()
  graph.makeKey("age").dataType(classOf[Integer]).make()
  graph.makeKey("reason").dataType(classOf[String]).make()
  graph.commit()

  // Titan graph elements
  val neptuneTitanVertex = {
    val vertex = graph.addVertex(null)
    vertex.setProperty("name", "neptune")
    vertex.setProperty("age", 4500)
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
    vertex.setProperty("name", "pluto")
    vertex.setProperty("age", 4000)
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
    new Edge(neptuneTitanVertex.getID, seaTitanVertex.getID, Property(gbID, neptuneTitanVertex.getID()), Property(gbID, seaTitanVertex.getID()), seaTitanEdge.getLabel(), gbSeaEdgeProperties)
  }

  val plutoGbEdge = new Edge(neptuneTitanVertex.getID, plutoTitanVertex.getID, Property(gbID, neptuneTitanVertex.getID()), Property(gbID, plutoTitanVertex.getID()), plutoTitanEdge.getLabel(), List[Property]())

  // Serialized Titan rows created using the Titan graph elements defined above.
  val titanRowMap = createTestTitanRows(graph)

  // Serialized HBase rows
  val hBaseRowMap = createTestHBaseRows(titanRowMap)


}


