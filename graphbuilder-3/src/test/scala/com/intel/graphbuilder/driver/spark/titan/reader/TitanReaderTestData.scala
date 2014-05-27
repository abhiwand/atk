package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.elements.{Edge, Property, Vertex}
import com.thinkaurelius.titan.graphdb.database.{EdgeSerializer, StandardTitanGraph}
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import com.thinkaurelius.titan.graphdb.internal._
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry
import com.thinkaurelius.titan.core.{TitanProperty, TitanElement, TitanVertex}
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler
import org.apache.commons.configuration.BaseConfiguration
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.HashMap


import TitanReaderConstants.TITAN_READER_GB_ID

object TitanReaderTestData {
  val gbID = TITAN_READER_GB_ID

  def openTitanInMemoryGraph(): StandardTitanGraph = {
    val conf = new BaseConfiguration()
    conf.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "inmemory")
    conf.setProperty("autotype", "none")
    new StandardTitanGraph(new GraphDatabaseConfiguration(conf))
  }

  def serializeGraphElement(titanEdgeSerializer: EdgeSerializer, titanVertex: TitanVertex, titanElement: TitanElement): Seq[Entry] = {
    val relation = titanElement.asInstanceOf[InternalRelation]

    val entryList = ListBuffer[Entry]()
    println("relation : type=" + relation.getType().getID() + " " + relation)
    for (pos <- 0 until relation.getLen()) {

      if (relation.getVertex(pos) == titanVertex) {
        entryList += titanEdgeSerializer.writeRelation(relation, pos, relation.tx())
      }
    }
    entryList.toSeq
  }

  def getSerializedEntryMap(graph: StandardTitanGraph): Map[String, TitanRow] = {
    val edgeSerializer = titanGraph.getEdgeSerializer()
    var entryMap = HashMap[String, TitanRow]()

    graph.getVertices().foreach(vertex => {
      val titanVertex = vertex.asInstanceOf[TitanVertex]
      val entryList = ListBuffer[Entry]()

      titanVertex.getEdges().foreach(edge => {
        entryList ++= serializeGraphElement(edgeSerializer, titanVertex, edge.asInstanceOf[TitanElement])
      })

      titanVertex.getProperties().foreach(property => {
        entryList ++= serializeGraphElement(edgeSerializer, titanVertex, property.asInstanceOf[TitanElement])
      })

      val titanRow = new TitanRow(IDHandler.getKey(titanVertex.getID), entryList.toSeq)
      entryMap += (vertex.getProperty("name").toString() -> titanRow)
    })

    entryMap
  }

  def getGbPropertyList(properties: Iterable[TitanProperty]): Seq[Property] = {
    properties.map(p => Property(p.getPropertyKey().getName(), p.getValue())).toList
  }

  val titanGraph = {
    val graph = openTitanInMemoryGraph()

    // Create a test graph which is a subgraph of Titan's graph of the gods
    graph.makeLabel("brother").make()
    graph.makeLabel("lives").make()
    graph.makeKey("name").dataType(classOf[String]).make()
    graph.makeKey("type").dataType(classOf[String]).make()
    graph.makeKey("age").dataType(classOf[Integer]).make()
    graph.makeKey("reason").dataType(classOf[String]).make()
    graph.commit()
    graph
  }


  val neptune = {
    val vertex = titanGraph.addVertex(null)
    vertex.setProperty("name", "neptune")
    vertex.setProperty("age", 4500)
    vertex.setProperty("type", "god")
    vertex
  }

  val sea = {
    val vertex = titanGraph.addVertex(null)
    vertex.setProperty("name", "sea")
    vertex.setProperty("type", "location")
    vertex
  }

  val pluto = {
    val vertex = titanGraph.addVertex(null)
    vertex.setProperty("name", "pluto")
    vertex.setProperty("age", 4000)
    vertex.setProperty("type", "god")
    vertex
  }

  val seaEdge = {
    val edge = neptune.addEdge("lives", sea)
    edge.setProperty("reason", "loves waves")
    edge
  }

  val plutoEdge = neptune.addEdge("brother", pluto)

  val titanRows = getSerializedEntryMap(titanGraph)

  val gbNeptune = {
    val gbNeptuneProperties = getGbPropertyList(neptune.getProperties())
    new Vertex(neptune.getID(), Property(gbID, neptune.getID()), gbNeptuneProperties)
  }

  val gbSea = {
    val gbSeaProperties = getGbPropertyList(sea.getProperties())
    new Vertex(sea.getID(), Property(gbID, sea.getID()), gbSeaProperties)
  }

  val gbPluto = {
    val gbPlutoProperties = getGbPropertyList(pluto.getProperties())
    new Vertex(pluto.getID(), Property(gbID, pluto.getID()), gbPlutoProperties)
  }

  val gbSeaEdge = {
    val gbSeaEdgeProperties = List(Property("reason", "loves waves"))
    new Edge(neptune.getID, sea.getID, Property(gbID, neptune.getID()), Property(gbID, sea.getID()), seaEdge.getLabel(), gbSeaEdgeProperties)
  }

  val gbPlutoEdge = new Edge(neptune.getID, pluto.getID, Property(gbID, neptune.getID()), Property(gbID, pluto.getID()), plutoEdge.getLabel(), List[Property]())


}
