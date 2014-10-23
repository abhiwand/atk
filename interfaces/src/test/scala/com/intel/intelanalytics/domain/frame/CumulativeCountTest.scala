package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class CumulativeCountTest extends WordSpec with MockitoSugar {

  "CumulativeCount" should {

    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { CumulativeCount(null, "sampleCol", "countVal") }
    }

    "not allow null sampleCol" in {
      intercept[IllegalArgumentException] { CumulativeCount(mock[FrameReference], null, "countVal") }
    }

    "not allow null countVal" in {
      intercept[IllegalArgumentException] { CumulativeCount(mock[FrameReference], "sampleCol", null) }
    }
  }
}
