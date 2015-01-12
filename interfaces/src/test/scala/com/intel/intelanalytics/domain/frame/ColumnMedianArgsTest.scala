package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class ColumnMedianArgsTest extends WordSpec with MockitoSugar {

  "ColumnMedian" should {
    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { ColumnMedianArgs(null, "dataColumn", None) }
    }
    "not allow null dataColumn" in {
      intercept[IllegalArgumentException] { ColumnMedianArgs(mock[FrameReference], null, None) }
    }
  }
}
