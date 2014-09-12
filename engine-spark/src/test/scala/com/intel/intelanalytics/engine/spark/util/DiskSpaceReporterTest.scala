package com.intel.intelanalytics.engine.spark.util

import org.scalatest.WordSpec

class DiskSpaceReporterTest extends WordSpec {

  "DiskSpaceReport" should {
    "not throw exceptions" in {
      DiskSpaceReporter.checkDiskSpace()
    }
  }
}
