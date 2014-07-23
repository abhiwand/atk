package com.intel.intelanalytics.engine.spark

import org.scalatest.FlatSpec

class SparkAutoPartitionerTest extends FlatSpec {

  val partitioner = new SparkAutoPartitioner(null)

  "SparkAutoPartitioner" should "calculate expected partitioning for VERY small files" ignore {
    assert(partitioner.partitionsFromFileSize(1) == 30)
  }

  ignore should "calculate the expected partitioning for small files" in {
    val tenMb = 10000000
    assert(partitioner.partitionsFromFileSize(tenMb) == 90)
  }

  ignore should "calculate max-partitions for VERY LARGE files" in {
    assert(partitioner.partitionsFromFileSize(Long.MaxValue) == 10000)
  }

}
