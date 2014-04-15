package com.intel.graphbuilder.schema

import org.specs2.mutable.Specification

class GraphSchemaSpec extends Specification {

  "GraphSchema" should {

    "be able to provide properties by name" in {
      val propDef1 = new PropertyDef(PropertyType.Vertex, "one", classOf[String], true, true)
      val propDef2 = new PropertyDef(PropertyType.Vertex, "two", classOf[String], false, false)
      val propDef3 = new PropertyDef(PropertyType.Edge, "three", classOf[String], false, false)
      val propDef4 = new PropertyDef(PropertyType.Vertex, "one", classOf[String], false, false)

      // invoke method under test
      val schema = new GraphSchema(Nil, List(propDef1, propDef2, propDef3, propDef4))

      // validations
      schema.propertiesWithName("one").size mustEqual 2
      schema.propertiesWithName("two").size mustEqual 1
      schema.propertiesWithName("three").size mustEqual 1
      schema.propertiesWithName("three").head.propertyType mustEqual PropertyType.Edge
    }
  }
}
