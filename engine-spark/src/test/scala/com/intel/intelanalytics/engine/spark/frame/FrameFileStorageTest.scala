package com.intel.intelanalytics.engine.spark.frame

import org.scalatest.FlatSpec
import com.intel.intelanalytics.engine.spark.SparkEngineConfig

class FrameFileStorageTest extends FlatSpec {

  val frameFileStorage = new FrameFileStorage(SparkEngineConfig.fsRoot, null)

  "FrameFileStorage" should "determine the correct data frames base directory" in {
    assert(frameFileStorage.frameBaseDirectory(1L).toString == "hdfs:/invalid-fsroot-host/user/iauser/intelanalytics/dataframes/1")
  }

  it should "determine the correct data frame revision directory" in {
    assert(frameFileStorage.frameRevisionDirectory(1L, 1).toString == "hdfs:/invalid-fsroot-host/user/iauser/intelanalytics/dataframes/1/rev1")
  }
}
