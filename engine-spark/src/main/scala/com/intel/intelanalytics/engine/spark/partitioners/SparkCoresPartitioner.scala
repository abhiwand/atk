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

import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import org.apache.spark.rdd.RDD

/**
 * Limits the number of partitions for an RDD based on the available spark cores.
 *
 * Some plugins (e.g., group-by) perform poorly if there are too many partitions relative to the
 * number of available Spark cores. This is more pronounced in the reduce phase.
 */
object SparkCoresPartitioner extends Serializable {

  /**
   * Get the number number of partitions based on available spark cores
   */
  def getNumPartitions[T](rdd: RDD[T]): Int = {
    val maxPartitions = getMaxSparkTasks(rdd)
    val numPartitions = Math.min(rdd.partitions.length, maxPartitions)

    //TODO: Replace print statement with IAT event context when event contexts are supported at workers
    println(s"Number of partitions computed by SparkCoresPartitioner: ${numPartitions}")
    numPartitions
  }

  // Get the maximum number of spark tasks to run
  private[spark] def getMaxSparkTasks[T](rdd: RDD[T]): Int = {
    val numExecutors = Math.max(1, rdd.sparkContext.getExecutorStorageStatus.size - 1)
    val numSparkCores = {
      val maxSparkCores = rdd.sparkContext.getConf.getInt("spark.cores.max", 0)
      if (maxSparkCores > 0) maxSparkCores else Runtime.getRuntime.availableProcessors() * numExecutors
    }
    SparkEngineConfig.maxTasksPerCore * numSparkCores
  }
}
