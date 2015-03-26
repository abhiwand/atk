//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.engine.spark.partitioners

import com.intel.intelanalytics.domain.schema.{ Column, FrameSchema, DataTypes, Schema }
import com.intel.intelanalytics.engine.spark.HdfsFileStorage
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.frame.FrameRdd
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

class SparkAutoPartitionerTest extends TestingSparkContextFlatSpec with MockitoSugar {

  val partitioner = new SparkAutoPartitioner(null)
  val rows = (1 to 100).map(i => Array(i.toLong, i.toString))
  val schema = FrameSchema(List(Column("num", DataTypes.int64), Column("name", DataTypes.string)))

  "SparkAutoPartitioner" should "calculate expected partitioning for VERY small files" in {
    assert(partitioner.partitionsFromFileSize(1) == 30)
  }

  it should "calculate the expected partitioning for small files" in {
    val tenMb = 10000000
    assert(partitioner.partitionsFromFileSize(tenMb) == 90)
  }

  it should "calculate max-partitions for VERY LARGE files" in {
    assert(partitioner.partitionsFromFileSize(Long.MaxValue) == 10000)
  }

  "repartitionFromFileSize" should "decrease the number of partitions" in {
    val fiveHundredKb = 50000 // Re-partitioner multiplies this size by frame-compression-ratio
    val mockHdfs = mock[HdfsFileStorage]
    when(mockHdfs.size("testpath")).thenReturn(fiveHundredKb)

    val repartitioner = new SparkAutoPartitioner(mockHdfs)
    val frameRdd = FrameRdd.toFrameRdd(schema, sparkContext.parallelize(rows, 180))

    val repartitionedRdd = repartitioner.repartition("testpath", frameRdd, false)
    assert(repartitionedRdd.partitions.length == 30)

    val repartitionedRdd2 = repartitioner.repartition("testpath", frameRdd, true)
    assert(repartitionedRdd2.partitions.length == 30)
  }

  "repartitionFromFileSize" should "increase the number of partitions when shuffle is true" in {
    val tenMb = 10000000 // Re-partitioner multiplies this size by frame-compression-ratio
    val mockHdfs = mock[HdfsFileStorage]
    when(mockHdfs.size("testpath")).thenReturn(tenMb)

    val repartitioner = new SparkAutoPartitioner(mockHdfs)
    val frameRdd = FrameRdd.toFrameRdd(schema, sparkContext.parallelize(rows, 30))

    val repartitionedRdd = repartitioner.repartition("testpath", frameRdd, true)
    assert(repartitionedRdd.partitions.length == 90)
  }

  "repartitionFromFileSize" should "not increase the number of partitions when shuffle is false" in {
    val tenMb = 10000000 // Re-partitioner multiplies this size by frame-compression-ratio
    val mockHdfs = mock[HdfsFileStorage]
    when(mockHdfs.size("testpath")).thenReturn(tenMb)

    val repartitioner = new SparkAutoPartitioner(mockHdfs)
    val frameRdd = FrameRdd.toFrameRdd(schema, sparkContext.parallelize(rows, 30))

    val repartitionedRdd = repartitioner.repartition("testpath", frameRdd, false)
    assert(repartitionedRdd.partitions.length == 30)
  }

  "repartitionFromFileSize" should "not re-partition if the percentage change is less than threshold" in {
    val tenMb = 10000000 // Re-partitioner multiplies this size by frame-compression-ratio
    val mockHdfs = mock[HdfsFileStorage]
    when(mockHdfs.size("testpath")).thenReturn(tenMb)

    val repartitioner = new SparkAutoPartitioner(mockHdfs)
    val frameRdd = FrameRdd.toFrameRdd(schema, sparkContext.parallelize(rows, 75))

    val repartitionedRdd = repartitioner.repartition("testpath", frameRdd, true)
    assert(repartitionedRdd.partitions.length == 75)
  }
}
