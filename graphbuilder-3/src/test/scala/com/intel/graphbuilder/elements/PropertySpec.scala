package com.intel.graphbuilder.elements

import org.specs2.mutable.Specification

class PropertySpec extends Specification {

  "Property" should {

    "merge 2 properties with the same key to 1" in {
      val p1 = new Property("keyA", "valueA")
      val p2 = new Property("keyA", "valueA")

      val result = Property.merge(List(p1), List(p2))

      result.size mustEqual 1
      result(0).key mustEqual "keyA"
      result(0).value mustEqual "valueA"
    }

    "merge 2 properties with different keys to 2" in {
      val p1 = new Property("keyA", "valueA")
      val p2 = new Property("keyB", "valueB")

      val result = Property.merge(List(p1), List(p2))

      result.size mustEqual 2
      result(0).key mustEqual "keyA"
      result(0).value mustEqual "valueA"
      result(1).key mustEqual "keyB"
      result(1).value mustEqual "valueB"
    }

    "merge 7 properties with mixture of same/different keys to 5" in {
      val p1 = new Property("keyA", "valueA")
      val p2 = new Property("keyB", "valueB")
      val p3 = new Property("keyC", "valueC")
      val p4 = new Property("keyB", "valueB2")
      val p5 = new Property("keyD", "valueD")
      val p6 = new Property("keyA", "valueA2")
      val p7 = new Property("keyE", "valueE")

      val result = Property.merge(List(p1, p2, p3, p4), List(p5, p6, p7))

      result.size mustEqual 5
    }

    "provide convenience constructor" in {
      val p = new Property(1, 2)
      p.key mustEqual "1" // key 1 is converted to a String
      p.value mustEqual 2
    }
  }
}
