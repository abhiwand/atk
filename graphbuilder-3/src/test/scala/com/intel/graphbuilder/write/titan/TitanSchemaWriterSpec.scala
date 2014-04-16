package com.intel.graphbuilder.write.titan

import org.specs2.mutable.Specification
import com.intel.graphbuilder.schema.{PropertyType, PropertyDef, EdgeLabelDef, GraphSchema}
import com.tinkerpop.blueprints.{Vertex, Direction, Edge}
import org.specs2.mock.Mockito
import com.thinkaurelius.titan.core.TitanGraph
import com.intel.graphbuilder.testutils.TestingTitan

class TitanSchemaWriterSpec extends Specification with Mockito {

  // before / after
  trait SchemaWriterSetup extends TestingTitan {
    lazy val titanSchemaWriter = new TitanSchemaWriter(graph)
  }

  "TitanSchemaWriter" should {

    "write an edge label definition" in new SchemaWriterSetup {
      // setup
      val edgeLabel = new EdgeLabelDef("myLabel")
      val schema = new GraphSchema(List(edgeLabel), Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("myLabel").isEdgeLabel mustEqual true
    }

    "write a property definition" in new SchemaWriterSetup {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val schema = new GraphSchema(Nil, List(propertyDef))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName").isPropertyKey mustEqual true
      graph.getType("propName").isUnique(Direction.IN) mustEqual false
    }

    "write a property definition that is unique and indexed" in new SchemaWriterSetup {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = true, indexed = true)
      val schema = new GraphSchema(Nil, List(propertyDef))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName").isPropertyKey mustEqual true
      graph.getType("propName").isUnique(Direction.IN) mustEqual true
    }

    "ignore duplicate property definitions" in new SchemaWriterSetup {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val propertyDup = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val schema = new GraphSchema(Nil, List(propertyDef, propertyDup))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName").isPropertyKey mustEqual true
      graph.getType("propName").isUnique(Direction.IN) mustEqual false
    }

    "handle empty lists" in new SchemaWriterSetup {
      val schema = new GraphSchema(Nil, Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName") must beNull
    }

    "require a graph" in {
      new TitanSchemaWriter(null) must throwA[IllegalArgumentException]
    }

    "require an open graph" in {
      // setup mocks
      val graph = mock[TitanGraph]
      graph.isOpen returns false

      // invoke method under test
      new TitanSchemaWriter(graph) must throwA[IllegalArgumentException]
    }

    "determine indexType for Edges " in {
      val graph = mock[TitanGraph]
      graph.isOpen returns true
      new TitanSchemaWriter(graph).indexType(PropertyType.Edge) mustEqual classOf[Edge]
    }

    "determine indexType for Vertices " in {
      val graph = mock[TitanGraph]
      graph.isOpen returns true
      new TitanSchemaWriter(graph).indexType(PropertyType.Vertex) mustEqual classOf[Vertex]
    }

    "fail for unexpected types" in {
      val graph = mock[TitanGraph]
      graph.isOpen returns true
      new TitanSchemaWriter(graph).indexType(new PropertyType.Value {
        override def id: Int = -99999999
      }) must throwA[RuntimeException]
    }
  }
}
