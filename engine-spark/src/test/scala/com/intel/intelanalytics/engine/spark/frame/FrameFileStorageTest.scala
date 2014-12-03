package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.engine.plugin.Call
import org.scalatest.FlatSpec

class FrameFileStorageTest extends FlatSpec {

  implicit val call = Call(null)
  val frameFileStorage = new FrameFileStorage("hdfs://hostname/user/iauser", null)

  "FrameFileStorage" should "determine the correct data frames base directory" in {
    assert(frameFileStorage.frameBaseDirectory(1L).toString == "hdfs://hostname/user/iauser/intelanalytics/dataframes/1")
  }

}
