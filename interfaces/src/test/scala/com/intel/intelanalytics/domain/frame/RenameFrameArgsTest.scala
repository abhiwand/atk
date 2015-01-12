package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class RenameFrameArgsTest extends WordSpec with MockitoSugar {

  "RenameFrame" should {

    "require a frame" in {
      intercept[IllegalArgumentException] { RenameFrameArgs(null, "newName") }
    }

    "require a name" in {
      intercept[IllegalArgumentException] { RenameFrameArgs(mock[FrameReference], null) }
    }
  }
}
