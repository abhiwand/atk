package com.intel.graphbuilder.elements

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito

class GbIdToPhysicalIdSpec extends Specification with Mockito {

  "GbIdToPhysicalId" should {

    "be able to be converted to a tuple" in {
      val gbId = mock[Property]
      val physicalId = new java.lang.Long(4)
      val gbToPhysical = new GbIdToPhysicalId(gbId, physicalId)
      gbToPhysical.toTuple._1 mustEqual gbId
      gbToPhysical.toTuple._2 mustEqual physicalId
    }
  }
}
