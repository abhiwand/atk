package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class TallyPercentArgsTest extends WordSpec with MockitoSugar {

  "CumulativePercentCount" should {

    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { TallyPercentArgs(null, "sampleCol", "countVal") }
    }

    "not allow null sampleCol" in {
      intercept[IllegalArgumentException] { TallyPercentArgs(mock[FrameReference], null, "countVal") }
    }

    "not allow null countVal" in {
      intercept[IllegalArgumentException] { TallyPercentArgs(mock[FrameReference], "sampleCol", null) }
    }
  }
}
