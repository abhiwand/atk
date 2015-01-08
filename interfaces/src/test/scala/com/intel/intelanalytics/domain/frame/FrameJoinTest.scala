package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class FrameJoinTest extends WordSpec with MockitoSugar {

  "FrameJoin" should {

    "require more non-null frames" in {
      intercept[IllegalArgumentException] { FrameJoin(null, "how") }
    }

    "require more than 0 frames" in {
      intercept[IllegalArgumentException] { FrameJoin(Nil, "how") }
    }

    "require more than 1 frame" in {
      intercept[IllegalArgumentException] { FrameJoin(List((1L, "frame")), "how") }
    }

    "work with 2 frames" in {
      FrameJoin(List((1L, "frame"), (2L, "another")), "how")
    }
  }
}
