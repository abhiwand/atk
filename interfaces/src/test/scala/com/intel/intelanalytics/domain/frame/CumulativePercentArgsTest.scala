package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class CumulativePercentArgsTest extends WordSpec with MockitoSugar {

  "CumulativePercentSum" should {

    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { CumulativePercentArgs(null, "sampleCol") }
    }

    "not allow null sampleCol" in {
      intercept[IllegalArgumentException] { CumulativePercentArgs(mock[FrameReference], null) }
    }
  }
}
