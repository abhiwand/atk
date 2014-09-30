//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

import com.intel.event.EventLogging

/**
 * Calculate a best guess for the number of partitions that should be used for loading this file into a Spark RDD.
 *
 * This number won't be perfect but should be better than using default.
 *
 * General Advice on Paritioning:
 * - Choose a reasonable number of partitions: no smaller than 100, no larger than 10,000 (large cluster)
 * - Lower bound: at least 2x number of cores in your cluster
 * - Upper bound: ensure your tasks take at least 100ms (if they are going faster, then you are probably spending more
 *   time scheduling tasks than executing them)
 * - Generally better to have slightly too many partitions than too few
 */
class SparkAutoPartitioner(fileStorage: HdfsFileStorage) extends EventLogging {

  /**
   * Calculate a best guess for the number of partitions that should be used for loading this file into a Spark RDD.
   *
   * This number won't be perfect but should be better than using default.
   *
   * @param path relative path
   * @return number of partitions that should be used for loading this file into a Spark RDD
   */
  def partitionsForFile(path: String): Int = withContext[Int]("spark-auto-partioning") {
    val size = fileStorage.size(path)
    val partitions = partitionsFromFileSize(size)
    info("auto partitioning path:" + path + ", size:" + size + ", partitions:" + partitions)
    partitions
  }

  /**
   * Get the partition count given a file size,
   * @param fileSize size of file in bytes
   * @return partition count that should be used
   */
  private[spark] def partitionsFromFileSize(fileSize: Long): Int = {
    var partitionCount = SparkEngineConfig.maxPartitions
    SparkEngineConfig.autoPartitionerConfig.foreach(partitionConfig => {
      if (fileSize <= partitionConfig.fileSizeUpperBound) {
        partitionCount = partitionConfig.partitionCount
      }
      else {
        // list is sorted, so we can exit early
        return partitionCount
      }
    })
    partitionCount
  }
}

/**
 * Map upper bounds of file size to partition sizes
 * @param fileSizeUpperBound upper bound on file size for the partitionCount in bytes
 * @param partitionCount number of partitions to use
 */
case class FileSizeToPartitionSize(fileSizeUpperBound: Long, partitionCount: Int)