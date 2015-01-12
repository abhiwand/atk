package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class ColumnFullStatisticsArgsTest extends WordSpec with MockitoSugar {

  "ColumnFullStatistics" should {
    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { ColumnFullStatisticsArgs(null, "dataColumn", None) }
    }
    "not allow null dataColumn" in {
      intercept[IllegalArgumentException] { ColumnFullStatisticsArgs(mock[FrameReference], null, None) }
    }
  }
}
