package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class ColumnSummaryStatisticsTest extends WordSpec with MockitoSugar {

  "ColumnSummaryStatistics" should {
    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { ColumnSummaryStatistics(null, "dataColumn", None, None) }
    }
    "not allow null dataColumn" in {
      intercept[IllegalArgumentException] { ColumnSummaryStatistics(mock[FrameReference], null, None, None) }
    }
  }
}
