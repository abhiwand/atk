package com.intel.graphbuilder.elements

import org.specs2.mutable.Specification

class EdgeSpec extends Specification {

  val tailId = new Property("gbId", 10001)
  val headId = new Property("gbId", 10002)
  val label = "myLabel"
  val edge = new Edge(tailId, headId, label, List(new Property("key", "value")))

  "Edge" should {

    "be reverse-able" in {
      // invoke method under test
      val reversedEdge = edge.reverse()

      // should be opposite
      edge.headVertexGbId mustEqual reversedEdge.tailVertexGbId
      edge.tailVertexGbId mustEqual reversedEdge.headVertexGbId

      // should be same same
      edge.label mustEqual reversedEdge.label
      edge.properties mustEqual reversedEdge.properties
    }

    "have a unique id made up of the tailId, headId, and label" in {
      edge.id mustEqual (tailId, headId, label)
    }

    "be mergeable" in {
      val edge2 = new Edge(tailId, headId, label, List(new Property("otherKey", "otherValue")))

      // invoke method under test
      val merged = edge.merge(edge2)

      merged.properties.size mustEqual 2
      merged.properties(0).key mustEqual "key"
      merged.properties(1).key mustEqual "otherKey"
    }

    "not allow merging of edges with different ids" in {
      val diffId = new Property("gbId", 9999)
      val edge2 = new Edge(tailId, diffId, label, List(new Property("otherKey", "otherValue")))

      edge.merge(edge2) must throwA[IllegalArgumentException]
    }
  }
}
