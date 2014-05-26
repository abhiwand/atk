package com.intel.graphbuilder.driver.spark.reader

import org.scalatest.WordSpec
import org.scalatest.Matchers
import com.intel.graphbuilder.driver.spark.titan.reader.TitanRelationFactory
import com.thinkaurelius.titan.graphdb.types.system.{SystemKey, SystemType}
import org.specs2.mock.Mockito

import com.thinkaurelius.titan.graphdb.types.vertices.{TitanLabelVertex, TitanKeyVertex}
import com.intel.graphbuilder.elements.{Edge, Property, Vertex}
import com.tinkerpop.blueprints.Direction

class TitanRelationFactoryTest extends WordSpec with Matchers with Mockito {

  "Creating a TitanRelationFactory" should {
    "result in a TitanRelationFactory instance with vertexID=12" in {
      val factory = new TitanRelationFactory(12)
      factory.getVertexID() shouldBe 12
    }
  }
  "Building a TitanRelationFactory" should {
    "ignore graph elements with type=SystemType" in {
      val factory = new TitanRelationFactory(100)
      val systemKey = mock[SystemKey]
      factory.setType(systemKey)
      factory.setValue("SystemProperty")
      factory.build()
      factory.createVertex() shouldBe None
    }
    "create a GraphBuilder vertex instance with vertexID=23, name=hercules, age=30" in {
      val vertexID = 23
      val nameKey = mock[TitanKeyVertex]
      val ageKey = mock[TitanKeyVertex]
      val nameProperty = Property("name", "hercules")
      val ageProperty = Property("age", 30)

      nameKey.getName() returns (nameProperty.key)
      ageKey.getName() returns (ageProperty.key)

      val factory = new TitanRelationFactory(vertexID)
      val vertex = new Vertex(vertexID, Property(factory.gbId, vertexID), List(nameProperty, ageProperty))

      factory.setType(nameKey)
      factory.setValue(nameProperty.value.asInstanceOf[Object])
      factory.build()

      factory.setType(ageKey)
      factory.setValue(30.asInstanceOf[Object])
      factory.build()
      factory.createVertex() shouldBe Some(vertex)
    }
    "create a GraphBuilder edge with srcVertexID=9, destVertexID=14, label=battled, time=12" in {
      val srcVertexID = 9
      val destVertexID = 14
      val edgeID = 42
      val edgeLabel = mock[TitanLabelVertex]
      val timeKey = mock[TitanKeyVertex]
      val timeProperty = Property("time", 12)

      edgeLabel.getName() returns ("battled")
      timeKey.getName() returns (timeProperty.key)

      val factory = new TitanRelationFactory(srcVertexID)
      val srcGbID = Property(factory.gbId, srcVertexID)
      val destGbID = Property(factory.gbId, destVertexID)
      val edge = new Edge(srcVertexID,destVertexID, srcGbID, destGbID, edgeLabel.getName(), List(timeProperty))

      factory.setRelationID(edgeID)
      factory.setType(edgeLabel)
      factory.setDirection(Direction.OUT)
      factory.setOtherVertexID(destVertexID)
      factory.addProperty(timeKey, timeProperty.value.asInstanceOf[Object])
      factory.build()
      factory.edgeList shouldBe List(edge)

    }
  }

}
