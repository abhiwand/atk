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

package com.intel.intelanalytics.engine.spark

import org.scalatest.FlatSpec

class SparkAutoPartitionerTest extends FlatSpec {

  val partitioner = new SparkAutoPartitioner(null)

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

}
