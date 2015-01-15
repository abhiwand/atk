package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class ColumnSummaryStatisticsArgsTest extends WordSpec with MockitoSugar {

  "ColumnSummaryStatistics" should {
    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { ColumnSummaryStatisticsArgs(null, "dataColumn", None, None) }
    }
    "not allow null dataColumn" in {
      intercept[IllegalArgumentException] { ColumnSummaryStatisticsArgs(mock[FrameReference], null, None, None) }
    }
  }
}
