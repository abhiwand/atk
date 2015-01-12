package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class CumulativeSumArgsTest extends WordSpec with MockitoSugar {

  "CumulativeSum" should {

    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { CumulativeSumArgs(null, "sampleCol") }
    }

    "not allow null sampleCol" in {
      intercept[IllegalArgumentException] { CumulativeSumArgs(mock[FrameReference], null) }
    }
  }
}
