package com.intel.spark.graphon.sampling

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar
import com.intel.intelanalytics.domain.graph.GraphReference

class VertexSampleArgumentsTest extends WordSpec with MockitoSugar {

  "VertexSampleArguments" should {
    "require sample size greater than or equal to 1" in {
      VertexSampleArguments(mock[GraphReference], 1, "uniform")
    }

    "require sample size greater than 0" in {
      intercept[IllegalArgumentException] { VertexSampleArguments(mock[GraphReference], 0, "uniform") }
    }

    "require a valid sample type" in {
      intercept[IllegalArgumentException] { VertexSampleArguments(mock[GraphReference], 2, "badvalue") }
    }
  }
}
