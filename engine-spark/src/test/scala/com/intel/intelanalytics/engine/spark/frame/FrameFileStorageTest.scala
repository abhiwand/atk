package com.intel.intelanalytics.engine.spark.frame

import org.scalatest.FlatSpec

class FrameFileStorageTest extends FlatSpec {

  val frameFileStorage = new FrameFileStorage("hdfs://hostname/user/iauser", null)

  "FrameFileStorage" should "determine the correct data frames base directory" in {
    assert(frameFileStorage.frameBaseDirectory(1L).toString == "hdfs://hostname/user/iauser/intelanalytics/dataframes/1")
  }

  it should "determine the correct data frame revision directory" in {
    assert(frameFileStorage.frameRevisionDirectory(1L, 1).toString == "hdfs://hostname/user/iauser/intelanalytics/dataframes/1/rev1")
  }
}
