package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.driver.spark.titan.reader.TitanReaderTestData._
import com.intel.graphbuilder.elements.GraphElement
import org.scalatest.{Matchers, WordSpec}
import org.specs2.mock.Mockito

class TitanRowParserTest extends WordSpec with Matchers with Mockito {

  "Parsing a TitanRow" should {
    "create a GraphBuilder vertex 'neptune' and two adjacent edges" in {
      val titanTransaction = titanGraph.newTransaction(titanGraph.buildTransaction())
      val titanRowParser = new TitanRowParser(titanRows.get("neptune").get, titanGraph.getEdgeSerializer(), titanTransaction)
      val graphElements = titanRowParser.parse()

      graphElements should contain theSameElementsAs List[GraphElement](gbNeptune, gbPlutoEdge, gbSeaEdge)
    }
    "create a GraphBuilder vertex 'pluto' with one adjacent edge" in {
      val titanTransaction = titanGraph.newTransaction(titanGraph.buildTransaction())
      val titanRowParser = new TitanRowParser(titanRows.get("pluto").get, titanGraph.getEdgeSerializer(), titanTransaction)
      val graphElements = titanRowParser.parse()

      graphElements should contain theSameElementsAs List[GraphElement](gbPluto, gbPlutoEdge)
    }
    "create a GraphBuilder vertex 'sea' with one adjacent edge" in {
      val titanTransaction = titanGraph.newTransaction(titanGraph.buildTransaction())
      val titanRowParser = new TitanRowParser(titanRows.get("sea").get, titanGraph.getEdgeSerializer(), titanTransaction)
      val graphElements = titanRowParser.parse()

      graphElements should contain theSameElementsAs List[GraphElement](gbSea, gbSeaEdge)
    }
  }

}
