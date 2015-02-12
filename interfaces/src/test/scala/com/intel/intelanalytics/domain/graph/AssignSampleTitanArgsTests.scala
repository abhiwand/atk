package com.intel.intelanalytics.domain.graph

import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, WordSpec }

class AssignSampleTitanArgsTests extends WordSpec with Matchers with MockitoSugar {

  "AssignSampleTitanArgs" should {
    "get a default Random seed" in {
      val args = new AssignSampleTitanArgs(mock[GraphReference], List(1), None, None, None)
      args.getRandomSeed should be(0)
    }
    "get default labels" in {
      val args = new AssignSampleTitanArgs(mock[GraphReference], List(0.3, 0.3, 0.2, 0.2), None, None, None)
      args.getSampleLabels should be(List("Sample_0", "Sample_1", "Sample_2", "Sample_3"))
    }
    "create a special label list if there are three percentages" in {
      val args = new AssignSampleTitanArgs(mock[GraphReference], List(0.3, 0.3, 0.3), None, None, None)
      args.getSampleLabels should be(List("TR", "TE", "VA"))
    }
    "get a default output property" in {
      val args = new AssignSampleTitanArgs(mock[GraphReference], List(0.3, 0.3, 0.3), None, None, None)
      args.getOutputProperty should be("sample_bin")
    }

    "require a frame reference" in {
      intercept[IllegalArgumentException] { AssignSampleTitanArgs(null, List(0)) }
    }

    "require sample percentages " in {
      intercept[IllegalArgumentException] { AssignSampleTitanArgs(mock[GraphReference], null) }
    }

    "require sample percentages greater than zero" in {
      intercept[IllegalArgumentException] { AssignSampleTitanArgs(mock[GraphReference], List(-1)) }
    }

    "require sample percentages sum less than one" in {
      intercept[IllegalArgumentException] { AssignSampleTitanArgs(mock[GraphReference], List(.33, .33, .5)) }
    }
  }

}
