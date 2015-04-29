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

package com.intel.graphbuilder.driver.spark.titan

import java.util.Random

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Map

/**
 * Broadcast variable for graph builder joins
 *
 * The broadcast variable is represented as a sequence of maps to allows us to support broadcast variables
 * larger than 2GB (current limit in Spark 1.2).
 *
 * @param rdd Pair RDD to broadcast
 *
 */
case class JoinBroadcastVariable[K, V](rdd: RDD[(K, V)]) {
  import JoinBroadcastVariable._
  //TODO: Create a base class for broadcast join variables to avoid code duplication once we move graphbuilder module to engine-spark
  require(rdd != null, "RDD should not be null")

  // Represented as a sequence of multi-maps to support broadcast variables larger than 2GB
  val broadcastMaps: Seq[Broadcast[Map[K, V]]] = createBroadcastMaps(rdd)

  /**
   * Get matching value from broadcast join variable using key
   *
   * @param key Join key
   * @return Optional matching value
   */
  def get(key: K): Option[V] = {
    var rowOption: Option[V] = None
    var i = 0
    val numMaps = length()

    do {
      rowOption = broadcastMaps(i).value.get(key)
      i = i + 1
    } while (i < numMaps && rowOption.isEmpty)

    rowOption
  }

  /**
   * Get matching value from broadcast join variable using key
   *
   * Throws exception if key is not present
   *
   * @param key Key
   * @return Matching value
   */
  def apply(key: K): V = get(key).getOrElse(throw new IllegalArgumentException(s"Could not retrieve key ${key}"))

  /**
   * Get length of broadcast variable
   *
   * @return length of sequence of broadcast multi-maps
   */
  def length(): Int = broadcastMaps.size

  // Create the broadcast variable for the join
  private def createBroadcastMaps(rdd: RDD[(K, V)]): Seq[Broadcast[Map[K, V]]] = {
    val rddSize = getRddSize(rdd)
    val broadcastList = rdd.collect().toList

    val numBroadcastVars = if (!broadcastList.isEmpty && rddSize > Int.MaxValue) {
      Math.ceil(rddSize.toDouble / Int.MaxValue).toInt // Limit size of each broadcast var to 2G (MaxInt)
    }
    else 1

    val broadcastMaps = listToMaps(broadcastList, numBroadcastVars)
    broadcastMaps.map(map => rdd.sparkContext.broadcast(map))
  }

  //Converts list to sequence of maps by randomly assigning list elements to maps.
  private def listToMaps[K, V](list: List[(K, V)], numMaps: Int): Seq[Map[K, V]] = {
    require(numMaps > 0, "Size of maps should exceed zero")
    val random = new Random(0) //Using seed to get deterministic results
    val maps = (0 until numMaps).map(_ => Map[K, V]()).toSeq

    list.foldLeft(maps) {
      case (maps, (key, value)) =>
        val i = random.nextInt(numMaps) //randomly split into multiple maps
        maps(i) += (key -> value)
        maps
    }
  }
}

object JoinBroadcastVariable {

  val rddCompressionFactor = 2.5d //Used to estimate actual size of RDD. Might move to config file

  /**
   * Get the estimated size of the RDD
   */
  def getRddSize[T](rdd: RDD[T]): Long = {
    val storageStatus = rdd.sparkContext.getExecutorStorageStatus
    val rddSize = rddCompressionFactor * storageStatus.map(status => status.memUsedByRdd(rdd.id)).sum
    println(s"Estimated rdd size=${rddSize}, executors=${storageStatus.length - 1}")
    rddSize.toLong
  }

  /**
   * Determine whether to use broadcast join for graph builder
   *
   * @param rdd RDD
   * @param broadcastJoinThreshold Use broadcast variable for join if size of one of the data frames is below threshold
   * @return True if size of RDD is below join threshold
   */
  def useBroadcastVariable[T](rdd: RDD[T], broadcastJoinThreshold: Long): Boolean = {
    val rddSize = getRddSize(rdd)

    val useBroadcastVariable = (rddSize > 0 && rddSize < broadcastJoinThreshold)
    if (useBroadcastVariable) {
      println(s"Using broadcast join: rdd size=${rddSize}, threshold=${broadcastJoinThreshold}")
    }
    else {
      println(s"Using hash join: rdd size=${rddSize}, threshold=${broadcastJoinThreshold}")
    }

    useBroadcastVariable
  }
}
