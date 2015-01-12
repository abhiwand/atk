package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class JoinArgsTest extends WordSpec with MockitoSugar {

  "FrameJoin" should {

    "require more non-null frames" in {
      intercept[IllegalArgumentException] { JoinArgs(null, "how") }
    }

    "require more than 0 frames" in {
      intercept[IllegalArgumentException] { JoinArgs(Nil, "how") }
    }

    "require more than 1 frame" in {
      intercept[IllegalArgumentException] { JoinArgs(List((1L, "frame")), "how") }
    }

    "work with 2 frames" in {
      JoinArgs(List((1L, "frame"), (2L, "another")), "how")
    }
  }
}
