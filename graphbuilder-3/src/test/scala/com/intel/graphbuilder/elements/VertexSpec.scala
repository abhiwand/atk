package com.intel.graphbuilder.elements

import org.specs2.mutable.Specification

class VertexSpec extends Specification {

  val gbId = new Property("gbId", 10001)
  val vertex = Vertex(gbId, List(new Property("key", "value")))

  "Vertex" should {
    "have a unique id that is the gbId" in {
      vertex.id mustEqual gbId
    }

    "be mergeable with another vertex" in {
      val vertex2 = new Vertex(gbId, List(new Property("anotherKey", "anotherValue")))

      // invoke method under test
      val merged = vertex.merge(vertex2)

      merged.gbId mustEqual gbId
      merged.properties.size mustEqual 2

      merged.properties(0).key mustEqual "key"
      merged.properties(0).value mustEqual "value"

      merged.properties(1).key mustEqual "anotherKey"
      merged.properties(1).value mustEqual "anotherValue"
    }

    "not allow null gbIds" in {
      new Vertex(null, Nil) must throwA[IllegalArgumentException]
    }

    "not allow merging of vertices with different ids" in {
      val diffId = new Property("gbId", 10002)
      val vertex2 = new Vertex(diffId, List(new Property("anotherKey", "anotherValue")))

      // invoke method under test
      vertex.merge(vertex2) must throwA[IllegalArgumentException]
    }
  }

}
