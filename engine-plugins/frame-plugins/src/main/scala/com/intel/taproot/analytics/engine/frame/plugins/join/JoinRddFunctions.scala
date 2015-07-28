/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.analytics.engine.frame.plugins.join

import com.intel.taproot.analytics.engine.frame.plugins.join.JoinRddImplicits._
import com.intel.taproot.analytics.engine.partitioners.SparkCoresPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

//implicit conversion for PairRDD

/**
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object JoinRddFunctions extends Serializable {

  /**
   * Perform join operation
   *
   * Supports left-outer joins, right-outer-joins, outer-joins, and inner joins
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @param broadcastJoinThreshold use broadcast variable for join if size of one of the data frames is below threshold
   * @param how join method
   * @return Joined RDD
   */
  def joinRDDs(left: RddJoinParam,
               right: RddJoinParam,
               how: String,
               broadcastJoinThreshold: Long = Long.MaxValue,
               skewedJoinType: Option[String] = None): RDD[Row] = {

    val result = how match {
      case "left" => leftOuterJoin(left, right, broadcastJoinThreshold, skewedJoinType)
      case "right" => rightOuterJoin(left, right, broadcastJoinThreshold, skewedJoinType)
      case "outer" => fullOuterJoin(left, right)
      case "inner" => innerJoin(left, right, broadcastJoinThreshold, skewedJoinType)
      case other => throw new IllegalArgumentException(s"Method $other not supported. only support left, right, outer and inner.")
    }

    result
  }

  /**
   * Perform inner join
   *
   * Inner joins return all rows with matching keys in the first and second data frame.
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @param broadcastJoinThreshold use broadcast variable for join if size of one of the data frames is below threshold
   *
   * @return Joined RDD
   */
  def innerJoin(left: RddJoinParam,
                right: RddJoinParam,
                broadcastJoinThreshold: Long,
                skewedJoinType: Option[String] = None): RDD[Row] = {
    // Estimated size in bytes used to determine whether or not to use a broadcast join
    val leftSizeInBytes = left.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val rightSizeInBytes = right.estimatedSizeInBytes.getOrElse(Long.MaxValue)

    val innerJoinedRDD = if (leftSizeInBytes < broadcastJoinThreshold || rightSizeInBytes < broadcastJoinThreshold) {
      left.innerBroadcastJoin(right, broadcastJoinThreshold)
    }
    else if (skewedJoinType.isDefined) {
      left.innerSkewedBroadcastJoin(right)
    }
    else {
      left.rdd.join(
        right.rdd,
        SparkCoresPartitioner.getNumPartitions(left.rdd)
      )
    }
    innerJoinedRDD.map {
      case (key, (leftValues, rightValues)) => {
        Row.merge(leftValues, rightValues)
      }
    }
  }

  /**
   * Perform full-outer join
   *
   * Full-outer joins return both matching, and non-matching rows in the first and second data frame.
   * Broadcast join is not supported.
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   *
   * @return Joined RDD
   */
  def fullOuterJoin(left: RddJoinParam, right: RddJoinParam): RDD[Row] = {
    left.rdd.fullOuterJoin(
      right.rdd,
      SparkCoresPartitioner.getNumPartitions(left.rdd)
    ).map {
        case (_, outerJoinResult) =>
          outerJoinResult match {
            case (Some(leftValues), Some(rightValues)) =>
              Row.merge(leftValues, rightValues)
            case (Some(leftValues), None) =>
              Row.fromSeq(leftValues.toSeq ++ (1 to right.columnCount).map(i => null))
            case (None, Some(rightValues)) =>
              Row.fromSeq((1 to left.columnCount).map(i => null) ++ rightValues.toSeq)
            case (None, None) =>
              throw new IllegalArgumentException("No join parameters were supplied")
          }
      }
  }

  /**
   * Perform right-outer join
   *
   * Right-outer joins return all the rows in the second data-frame, and matching rows in the first data frame.
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @param broadcastJoinThreshold use broadcast variable for join if size of first data frame is below threshold
   *
   * @return Joined RDD
   */
  def rightOuterJoin(left: RddJoinParam,
                     right: RddJoinParam,
                     broadcastJoinThreshold: Long,
                     skewedJoinType: Option[String] = None): RDD[Row] = {
    // Estimated size in bytes used to determine whether or not to use a broadcast join
    val leftSizeInBytes = left.estimatedSizeInBytes.getOrElse(Long.MaxValue)

    val rightJoinedRDD = if (leftSizeInBytes < broadcastJoinThreshold) {
      left.rightBroadcastJoin(right)
    }
    else if (skewedJoinType.isDefined) {
      left.rightSkewedBroadcastJoin(right)
    }
    else {
      left.rdd.rightOuterJoin(
        right.rdd,
        SparkCoresPartitioner.getNumPartitions(left.rdd)
      )
    }
    rightJoinedRDD.map {
      case (_, (leftValues, rightValues)) => {
        leftValues match {
          case s: Some[Row] => Row.merge(s.get, rightValues)
          case None => Row.fromSeq((1 to left.columnCount).map(i => null) ++ rightValues.toSeq)
        }
      }
    }
  }

  /**
   * Perform left-outer join
   *
   * Left-outer joins return all the rows in the first data-frame, and matching rows in the second data frame.
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @param broadcastJoinThreshold use broadcast variable for join if size of second data frame is below threshold
   *
   * @return Joined RDD
   */
  def leftOuterJoin(left: RddJoinParam,
                    right: RddJoinParam,
                    broadcastJoinThreshold: Long,
                    skewedJoinType: Option[String] = None): RDD[Row] = {
    val rightSizeInBytes = right.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val leftJoinedRDD = skewedJoinType match {
      case Some("skewedbroadcast") => left.leftSkewedBroadcastJoin(right)
      case Some("skewedhash") => left.leftSkewedHashJoin(right)
      case x if rightSizeInBytes < broadcastJoinThreshold => left.leftBroadcastJoin(right)
      case _ => left.rdd.leftOuterJoin(right.rdd, SparkCoresPartitioner.getNumPartitions(left.rdd))
    }
    leftJoinedRDD.map {
      case (_, (leftValues, rightValues)) => {
        rightValues match {
          case s: Some[Row] => Row.merge(leftValues, s.get)
          case None => Row.fromSeq(leftValues.toSeq ++ (1 to right.columnCount).map(i => null))
        }
      }
    }
  }
}
