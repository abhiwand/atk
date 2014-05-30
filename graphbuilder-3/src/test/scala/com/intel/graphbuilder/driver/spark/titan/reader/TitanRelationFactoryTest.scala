package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.elements.{GraphElement, Edge, Property, Vertex}
import com.thinkaurelius.titan.graphdb.types.system.SystemKey
import com.thinkaurelius.titan.graphdb.types.vertices.{TitanLabelVertex, TitanKeyVertex}
import com.tinkerpop.blueprints.Direction
import org.scalatest.{WordSpec, Matchers}
import org.specs2.mock.Mockito
import com.thinkaurelius.titan.graphdb.types.IndexType
import com.thinkaurelius.titan.graphdb.internal.InternalType

object TitanRelationFactoryTest extends Mockito {
  // Mock vertices
  val vertexID = 23
  val nameKey = mock[TitanKeyVertex]
  val ageKey = mock[TitanKeyVertex]
  val nameProperty = Property("name", "hercules")
  val ageProperty = Property("age", 30)

  nameKey.getName() returns (nameProperty.key)
  ageKey.getName() returns (ageProperty.key)

  val vertex = new Vertex(vertexID, Property(TitanReader.TITAN_READER_GB_ID, vertexID), List(nameProperty, ageProperty))

  // Mock edges
  val edgeLabel = mock[TitanLabelVertex]
  edgeLabel.getName() returns ("battled")
  val timeKey = mock[TitanKeyVertex]
  timeKey.getName() returns ("time")

  val destVertexID1 = 14
  val edgeID1 = 42
  val timeProperty1 = Property("time", 12)

  val destVertexID2 = 32
  val edgeID2 = 89
  val timeProperty2 = Property("time", 67)

  val srcGbID = Property(TitanReader.TITAN_READER_GB_ID, vertexID)
  val destGbID1 = Property(TitanReader.TITAN_READER_GB_ID, destVertexID1)
  val destGbID2 = Property(TitanReader.TITAN_READER_GB_ID, destVertexID2)
  val edgeList = List(
    new Edge(vertexID, destVertexID1, srcGbID, destGbID1, edgeLabel.getName(), List(timeProperty1)),
    new Edge(vertexID, destVertexID2, srcGbID, destGbID2, edgeLabel.getName(), List(timeProperty2)))
}

class TitanRelationFactoryTest extends WordSpec with Matchers  {

  import TitanRelationFactoryTest._

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
      val factory = new TitanRelationFactory(10)
      factory.build()
      factory.createGraphElements() shouldBe Seq.empty[GraphElement]
    }
    "ignore graph elements with type=SystemType" in {
      val factory = new TitanRelationFactory(100)
      val systemKey = mock[SystemKey]
      factory.setType(systemKey)
      factory.setValue("SystemProperty")
      factory.build()
      factory.createGraphElements() shouldBe Seq.empty[GraphElement]
    }
    "throw an IllegalArgumentException when the TitanType is not system, edge label, or vertex property" in {
      intercept[IllegalArgumentException] {
        val factory = new TitanRelationFactory(100)
        val invalidType = mock[InternalType]
        invalidType.isEdgeLabel returns (false)
        invalidType.isPropertyKey returns (false)
        factory.setType(invalidType)
        factory.build()
      }
    }
    "create a GraphBuilder vertex instance with vertexID=23, name=hercules, age=30" in {
      val factory = new TitanRelationFactory(vertexID)

      factory.setType(nameKey)
      factory.setValue(nameProperty.value.asInstanceOf[Object])
      factory.build()

      factory.setType(ageKey)
      factory.setValue(ageProperty.value.asInstanceOf[Object])
      factory.build()
      factory.createGraphElements() should contain theSameElementsAs List(vertex)
    }
    "create a GraphBuilder vertex with two edges" in {

      val factory = new TitanRelationFactory(vertexID)

      // create vertex
      factory.setType(nameKey)
      factory.setValue(nameProperty.value.asInstanceOf[Object])
      factory.build()
      factory.setType(ageKey)
      factory.setValue(ageProperty.value.asInstanceOf[Object])
      factory.build()

      // create edges
      edgeList.map (edge => {
        val edgeProperty = edge.properties(0)
        val dummyEdgeId = 1
        factory.setRelationID(dummyEdgeId)
        factory.setType(edgeLabel)
        factory.setDirection(Direction.OUT)
        factory.setOtherVertexID(edge.headPhysicalId.asInstanceOf[Int].asInstanceOf[Long])
        factory.addProperty(timeKey, edgeProperty.value.asInstanceOf[Object])
        factory.build()
      })

      factory.createGraphElements() should contain theSameElementsAs edgeList :+ vertex

    }

  }
}
