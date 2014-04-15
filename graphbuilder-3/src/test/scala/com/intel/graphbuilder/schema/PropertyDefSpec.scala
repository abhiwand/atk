package com.intel.graphbuilder.schema

import org.specs2.mutable.Specification

class PropertyDefSpec extends Specification {

  "PropertyDef" should {

    "require a name" in {
      new PropertyDef(PropertyType.Vertex, null, null, false, false) must throwA[IllegalArgumentException]
    }
  }
}
