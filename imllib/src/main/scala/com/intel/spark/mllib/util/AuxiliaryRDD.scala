//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

package com.intel.spark.mllib.util

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import java.util.Random
import org.apache.spark.{Partition, TaskContext}

private[spark]
class AuxiliaryRDDPartition(val prev: Partition, val seed: Int) extends Partition with Serializable {
  override val index: Int = prev.index
}

/**
 * Class to generate auxilary RDD from an input RDD. The generated RDD has the same
 * number of partitions as the input RDD, and each partition has the same number of entries
 * as the corresponding partition of input RDD. The entry values in auxilary RDD are random
 * samples from a uniform distribution on [0, 1]. 
 *
 * @param prev: An input RDD as a blueprint.
 * @param seed: Random seed for random number generator.
 */
class AuxiliaryRDD[T: ClassTag](
    prev: RDD[T],
    seed: Int)
  extends RDD[Double](prev) {

  override def getPartitions: Array[Partition] = {
    val rg = new Random(seed)
    firstParent[T].partitions.map(x => new AuxiliaryRDDPartition(x, rg.nextInt))
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[AuxiliaryRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[Double] = {
    val split = splitIn.asInstanceOf[AuxiliaryRDDPartition]
    val rand = new Random(split.seed)
    firstParent[T].iterator(split.prev, context).map(x => rand.nextDouble)
  }
}
