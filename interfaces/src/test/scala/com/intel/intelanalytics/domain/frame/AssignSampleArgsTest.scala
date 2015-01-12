package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class AssignSampleArgsTest extends WordSpec with MockitoSugar {

  "AssignSample" should {

    "require a frame reference" in {
      intercept[IllegalArgumentException] { AssignSampleArgs(null, List(0)) }
    }

    "require sample percentages " in {
      intercept[IllegalArgumentException] { AssignSampleArgs(mock[FrameReference], null) }
    }

    "require sample percentages greater than zero" in {
      intercept[IllegalArgumentException] { AssignSampleArgs(mock[FrameReference], List(-1)) }
    }

    "require sample percentages sum less than one" in {
      intercept[IllegalArgumentException] { AssignSampleArgs(mock[FrameReference], List(.33, .33, .5)) }
    }
  }
}
