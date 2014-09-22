package com.intel.intelanalytics.engine.spark

import org.scalatest.FlatSpec

class SparkEngineConfigTest extends FlatSpec {

  "SparkEngineConfig" should "provide the expected maxPartitions size" in {
    assert(SparkEngineConfig.maxPartitions == 10000, "data bricks recommended 10,000 as the max partition value, please don't change unless you are decreasing")
  }

  it should "provide auto partitioner config in the expected order" in {
    val list = SparkEngineConfig.autoPartitionerConfig

    // list should have something in it
    assert(list.size > 3)

    // validate file size is decreasing
    var fileSize = Long.MaxValue
    list.foreach(item => {
      assert(item.fileSizeUpperBound < fileSize)
      fileSize = item.fileSizeUpperBound
    })
  }

  it should "provide auto partitioner config with the expected first element" in {
    val list = SparkEngineConfig.autoPartitionerConfig

    assert(list.size > 3)

    val sixHundredGB = 600000000000L
    assert(list.head.fileSizeUpperBound == sixHundredGB)
    assert(list.head.partitionCount == 7500)
  }

  it should "provide auto partitioner config with the expected last element" in {
    val list = SparkEngineConfig.autoPartitionerConfig

    assert(list.size > 3)
    assert(list.last.fileSizeUpperBound == 1000000)
    assert(list.last.partitionCount == 30)
  }
}
