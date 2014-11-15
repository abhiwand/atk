package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class AssignSampleTest extends WordSpec with MockitoSugar {

  "AssignSample" should {

    "require a frame reference" in {
      intercept[IllegalArgumentException] { AssignSample(null, List(0)) }
    }

    "require sample percentages " in {
      intercept[IllegalArgumentException] { AssignSample(mock[FrameReference], null) }
    }

    "require sample percentages greater than zero" in {
      intercept[IllegalArgumentException] { AssignSample(mock[FrameReference], List(-1)) }
    }

    "require sample percentages sum less than one" in {
      intercept[IllegalArgumentException] { AssignSample(mock[FrameReference], List(.33, .33, .5)) }
    }
  }
}
