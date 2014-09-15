package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class ColumnMedianTest extends WordSpec with MockitoSugar {

  "ColumnMedian" should {
    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { ColumnMedian(null, "dataColumn", None) }
    }
    "not allow null dataColumn" in {
      intercept[IllegalArgumentException] { ColumnMedian(mock[FrameReference], null, None) }
    }
  }
}
