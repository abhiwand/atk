package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class CumulativePercentSumTest extends WordSpec with MockitoSugar {

  "CumulativePercentSum" should {

    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { CumulativePercentSum(null, "sampleCol") }
    }

    "not allow null sampleCol" in {
      intercept[IllegalArgumentException] { CumulativePercentSum(mock[FrameReference], null) }
    }
  }
}
