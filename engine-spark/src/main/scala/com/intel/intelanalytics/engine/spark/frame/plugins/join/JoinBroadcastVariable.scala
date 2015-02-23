//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014-2015 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.frame.plugins.join

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._

import scala.collection.mutable.{ HashMap, MultiMap, Set }
import scala.util.Random

/**
 * Broadcast variable for joins
 *
 * The broadcast variable is represented as a sequence of multi-maps. Multi-maps allow us to
 * support duplicate keys during a join. The key in the multi-map is the join key, and the value is row.
 * Representing the broadcast variable as a sequence of multi-maps allows us to support broadcast variables
 * larger than 2GB (current limit in Spark 1.2).
 *
 * @param joinParam Join parameter for data frame
 */
case class JoinBroadcastVariable(joinParam: RDDJoinParam) {
  require(joinParam != null, "Join parameter should not be null")

  // Represented as a sequence of multi-maps to support broadcast variables larger than 2GB
  // Using multi-maps instead of hash maps so that we can support duplicate keys.
  val broadcastMultiMaps: Seq[Broadcast[MultiMap[Any, Row]]] = createBroadcastMultiMaps(joinParam)

  /**
   * Get matching set of rows by key from broadcast join variable
   *
   * @param key Join key
   * @return Matching set of rows if found. Multiple rows might match if there are duplicate keys.
   */
  def get(key: Any): Option[Set[Row]] = {
    var rowOption: Option[Set[Row]] = None
    var i = 0
    val numMultiMaps = length()

    do {
      rowOption = broadcastMultiMaps(i).value.get(key)
      i = i + 1
    } while (i < numMultiMaps && rowOption.isEmpty)

    rowOption
  }

  /**
   * Get length of broadcast variable
   *
   * @return length of sequence of broadcast multi-maps
   */
  def length(): Int = broadcastMultiMaps.size

  // Create the broadcast variable for the join
  private def createBroadcastMultiMaps(joinParam: RDDJoinParam): Seq[Broadcast[MultiMap[Any, Row]]] = {
    //Grouping by key to ensure that duplicate keys are not split across different broadcast variables
    val broadcastList = joinParam.rdd.groupByKey().collect().toList

    val rddSizeInBytes = joinParam.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val numBroadcastVars = if (!broadcastList.isEmpty && rddSizeInBytes < Long.MaxValue && rddSizeInBytes > 0) {
      Math.ceil(rddSizeInBytes.toDouble / Int.MaxValue).toInt // Limit size of each broadcast var to 2G (MaxInt)
    }
    else 1

    val broadcastMultiMaps = listToMultiMap(broadcastList, numBroadcastVars)
    broadcastMultiMaps.map(map => joinParam.rdd.sparkContext.broadcast(map))
  }

  //Converts list to sequence of multi-maps by randomly assigning list elements to multi-maps.
  //Broadcast variables are stored as multi-maps to ensure results are not lost when RDD has duplicate keys
  private def listToMultiMap(list: List[(Any, Iterable[Row])], numMultiMaps: Int): Seq[MultiMap[Any, Row]] = {
    require(numMultiMaps > 0, "Size of multimap should exceed zero")
    val random = new Random(0) //Using seed to get deterministic results
    val multiMaps = (0 until numMultiMaps).map(_ => new HashMap[Any, Set[Row]] with MultiMap[Any, Row]).toSeq

    list.foldLeft(multiMaps) {
      case (maps, (key, rows)) =>
        val i = random.nextInt(numMultiMaps) //randomly split into multiple multi-maps
        rows.foreach(row => maps(i).addBinding(key, row))
        maps
    }
  }
}

