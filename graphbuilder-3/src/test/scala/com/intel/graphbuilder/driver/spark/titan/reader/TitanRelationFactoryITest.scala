package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.elements.{ GraphElement, Property }
import com.thinkaurelius.titan.graphdb.types.system.SystemKey
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry
import org.scalatest.{ WordSpec, Matchers }
import org.specs2.mock.Mockito
import com.thinkaurelius.titan.graphdb.internal.InternalType
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler
import TitanReaderTestData._

/**
 * Integration test for Titan relation factory.
 */
class TitanRelationFactoryITest extends WordSpec with Matchers with Mockito {

  val edgeSerializer = graph.getEdgeSerializer()

  "Creating a TitanRelationFactory" should {
    "result in a TitanRelationFactory instance with vertexID=12" in {
      val factory = new TitanRelationFactory(12)
      factory.getVertexID() shouldBe 12
    }
    "throw an IllegalArgumentException when vertexID is less than or equal to zero" in {
      intercept[IllegalArgumentException] {
        new TitanRelationFactory(-1)
      }
    }
  }
  "Building a TitanRelationFactory" should {
    "return an empty list if no vertex properties or edges are set" in {
      val vertexID = 10
      val factory = new TitanRelationFactory(vertexID)
      val titanRow = new TitanRow(IDHandler.getKey(vertexID), Seq.empty[Entry])
      val graphElements = factory.createGraphElements(titanRow, edgeSerializer, transaction)
      graphElements shouldBe Seq.empty[GraphElement]
    }
    "ignore graph elements with type=SystemType" in {
      val factory = new TitanRelationFactory(100)
      val systemKey = mock[SystemKey]
      factory.setType(systemKey)
      factory.setValue("SystemProperty")
      factory.build()
      factory.vertexProperties shouldBe List.empty[Property]
    }
    "throw an IllegalArgumentException when building a TitanType is not system, edge label, or vertex property" in {
      intercept[IllegalArgumentException] {
        val factory = new TitanRelationFactory(100)
        val invalidType = mock[InternalType]
        invalidType.isEdgeLabel returns (false)
        invalidType.isPropertyKey returns (false)
        factory.setType(invalidType)
        factory.build()
      }
    }
    "create a GraphBuilder vertex 'neptune' with two outgoing edges" in {
      val vertexID = neptuneGbVertex.physicalId.asInstanceOf[Long]
      val factory = new TitanRelationFactory(vertexID)
      val graphElements = factory.createGraphElements(titanRowMap.get("neptune").get, graph.getEdgeSerializer(), transaction)

      graphElements should contain theSameElementsAs List[GraphElement](neptuneGbVertex, plutoGbEdge, seaGbEdge)
    }
  }
}
