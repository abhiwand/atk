package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class TallyArgsTest extends WordSpec with MockitoSugar {

  "CumulativeCount" should {

    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { TallyArgs(null, "sampleCol", "countVal") }
    }

    "not allow null sampleCol" in {
      intercept[IllegalArgumentException] { TallyArgs(mock[FrameReference], null, "countVal") }
    }

    "not allow null countVal" in {
      intercept[IllegalArgumentException] { TallyArgs(mock[FrameReference], "sampleCol", null) }
    }
  }
}
