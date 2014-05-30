package com.intel.graphbuilder.driver.spark.titan.reader


import com.intel.graphbuilder.elements.GraphElement
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.{StaticBufferEntry, Entry}
import org.scalatest.{Matchers, WordSpec}
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler
import com.intel.graphbuilder.testutils.TitanReaderTestData

class TitanRowParserITest extends WordSpec with Matchers {

  import TitanReaderTestData._

  "Parsing a TitanRow" should {

    "throw a RuntimeException when the row is not serialized in a format that Titan understands" in {
      intercept[RuntimeException] {
        val titanTransaction = graph.newTransaction(graph.buildTransaction())
        val invalidEntries = List(StaticBufferEntry.of(new StaticArrayBuffer("key".getBytes()), new StaticArrayBuffer("value".getBytes())))
        val invalidTitanRow = new TitanRow(IDHandler.getKey(1), invalidEntries)
        val titanRowParser = new TitanRowParser(invalidTitanRow, graph.getEdgeSerializer(), titanTransaction)
        titanRowParser.parse()
      }
    }
    "return an empty list of GraphElement type when serialized entries is empty" in {
      val titanTransaction = graph.newTransaction(graph.buildTransaction())
      val titanRow = new TitanRow(IDHandler.getKey(1), Seq.empty[Entry])
      val titanRowParser = new TitanRowParser(titanRow, graph.getEdgeSerializer(), titanTransaction)
      val graphElements = titanRowParser.parse()

      graphElements shouldBe (Seq.empty[GraphElement])
    }
    "create a GraphBuilder vertex 'neptune' and two adjacent edges" in {
      val titanTransaction = graph.newTransaction(graph.buildTransaction())
      val titanRowParser = new TitanRowParser(titanRowMap.get("neptune").get, graph.getEdgeSerializer(), titanTransaction)
      val graphElements = titanRowParser.parse()

      graphElements should contain theSameElementsAs List[GraphElement](neptuneGbVertex, plutoGbEdge, seaGbEdge)
    }
    "create a GraphBuilder vertex 'pluto' with one adjacent edge" in {
      val titanTransaction = graph.newTransaction(graph.buildTransaction())
      val titanRowParser = new TitanRowParser(titanRowMap.get("pluto").get, graph.getEdgeSerializer(), titanTransaction)
      val graphElements = titanRowParser.parse()

      graphElements should contain theSameElementsAs List[GraphElement](plutoGbVertex, plutoGbEdge)
    }

  }

}
