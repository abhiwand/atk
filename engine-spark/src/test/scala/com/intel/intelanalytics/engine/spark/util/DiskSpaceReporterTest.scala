package com.intel.intelanalytics.engine.spark.util

import com.intel.intelanalytics.engine.plugin.Call
import org.scalatest.WordSpec

class DiskSpaceReporterTest extends WordSpec {
  implicit val call = Call(null)

  "DiskSpaceReport" should {
    "not throw exceptions" in {
      DiskSpaceReporter.checkDiskSpace()
    }
  }
}
