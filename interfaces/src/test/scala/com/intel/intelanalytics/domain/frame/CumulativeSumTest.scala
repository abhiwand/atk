package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class CumulativeSumTest extends WordSpec with MockitoSugar {

  "CumulativeSum" should {

    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { CumulativeSum(null, "sampleCol") }
    }

    "not allow null sampleCol" in {
      intercept[IllegalArgumentException] { CumulativeSum(mock[FrameReference], null) }
    }
  }
}
