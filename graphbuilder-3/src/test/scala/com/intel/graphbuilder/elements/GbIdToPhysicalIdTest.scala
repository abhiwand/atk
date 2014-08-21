package com.intel.graphbuilder.elements

import org.scalatest.{ Matchers, WordSpec }
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

class GbIdToPhysicalIdTest extends WordSpec with Matchers with MockitoSugar {

  "GbIdToPhysicalId" should {

    "be able to be converted to a tuple" in {
      val gbId = mock[Property]
      val physicalId = new java.lang.Long(4)
      val gbToPhysical = new GbIdToPhysicalId(gbId, physicalId)
      gbToPhysical.toTuple._1 shouldBe gbId
      gbToPhysical.toTuple._2 shouldBe physicalId
    }
  }
}
