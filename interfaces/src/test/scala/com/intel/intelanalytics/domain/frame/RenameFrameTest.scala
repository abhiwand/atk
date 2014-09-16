package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class RenameFrameTest extends WordSpec with MockitoSugar {

  "RenameFrame" should {

    "require a frame" in {
      intercept[IllegalArgumentException] { RenameFrame(null, "newName") }
    }

    "require a name" in {
      intercept[IllegalArgumentException] { RenameFrame(mock[FrameReference], null) }
    }
  }
}
