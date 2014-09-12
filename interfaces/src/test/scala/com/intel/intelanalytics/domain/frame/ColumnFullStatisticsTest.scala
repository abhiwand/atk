package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class ColumnFullStatisticsTest extends WordSpec with MockitoSugar {

  "ColumnFullStatistics" should {
    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { ColumnFullStatistics(null, "dataColumn", None) }
    }
    "not allow null dataColumn" in {
      intercept[IllegalArgumentException] { ColumnFullStatistics(mock[FrameReference], null, None) }
    }
  }
}
