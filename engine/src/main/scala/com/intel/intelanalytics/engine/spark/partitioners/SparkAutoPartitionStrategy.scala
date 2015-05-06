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

/**
 * Partitioning Strategies for Spark RDDs.
 *
 * Uses Spark's coalesce() to re-partition RDDs.
 *
 * @see org.apache.spark.rdd.RDD
 */
object SparkAutoPartitionStrategy {

  /**
   * Partitioning Strategies for Spark RDDs
   * @param name Partition strategy name
   */
  sealed abstract class PartitionStrategy(val name: String) {
    override def toString = name
  }

  /**
   * Disable repartitioning of Spark RDDs.
   */
  case object Disabled extends PartitionStrategy("DISABLED")

  /**
   * Repartition RDDs only when the requested number of partitions is less than the current partitions.
   *
   * Shrinking RDD partitions is less expensive and does not involve a shuffle operation.
   *
   * @see org.apache.spark.rdd.RDD#coalesce(Int, Boolean)
   */
  case object ShrinkOnly extends PartitionStrategy("SHRINK_ONLY")

  /**
   * Repartition RDDs only when the requested number of partitions is less or greater than the current partitions.
   *
   * Uses more-expensive Spark shuffle operation to shrink or grow partitions.
   *
   * @see org.apache.spark.rdd.RDD#coalesce(Int, Boolean)
   */
  case object ShrinkOrGrow extends PartitionStrategy("SHRINK_OR_GROW")

  val partitionStrategies: Seq[PartitionStrategy] = Seq(Disabled, ShrinkOnly, ShrinkOrGrow)

  /**
   * Find mode by name
   *
   * @param name Name of mode
   * @return Matching mode. If not found, defaults to Disabled
   */
  def getRepartitionStrategy(name: String): PartitionStrategy = {
    val strategy = partitionStrategies.find(m => m.name.equalsIgnoreCase(name)).getOrElse({
      throw new IllegalArgumentException(s"Unsupported Spark auto-partitioning strategy: ${name}")
    })
    strategy
  }
}
