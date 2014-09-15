package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class CumulativePercentCountTest extends WordSpec with MockitoSugar {

  "CumulativePercentCount" should {

    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { CumulativePercentCount(null, "sampleCol", "countVal") }
    }

    "not allow null sampleCol" in {
      intercept[IllegalArgumentException] { CumulativePercentCount(mock[FrameReference], null, "countVal") }
    }

    "not allow null countVal" in {
      intercept[IllegalArgumentException] { CumulativePercentCount(mock[FrameReference], "sampleCol", null) }
    }
  }
}
