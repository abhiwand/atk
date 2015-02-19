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

package com.intel.intelanalytics.engine.spark.frame.plugins.join

import com.intel.intelanalytics.domain.schema.DataTypes
import org.apache.spark.engine.Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

//implicit conversion for PairRDD

import org.apache.spark.SparkContext._

import scala.collection.mutable.{ HashMap, MultiMap, Set }

/**
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object JoinRDDFunctions extends Serializable {

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
  def joinRDDs(left: RDDJoinParam, right: RDDJoinParam, how: String, broadcastJoinThreshold: Long = Long.MaxValue): RDD[Row] = {

    //Ordering of Any is needed to enable Spark's memory-efficient sort-based shuffle.
    //TODO: Test partitioning in Spark 1.2.0+ once memory bugs in Spark's shuffle are fixed
    //TODO: Move to DataTypes or DataFrame if sort-based shuffle works well so that we can enable sort-based shuffle for plugins
    implicit val anyOrdering = new Ordering[Any] {
      def compare(a: Any, b: Any) = DataTypes.compare(a, b)
    }

    val result = how match {
      case "left" => leftOuterJoin(left, right, broadcastJoinThreshold)
      case "right" => rightOuterJoin(left, right, broadcastJoinThreshold)
      case "outer" => fullOuterJoin(left, right)
      case "inner" => innerJoin(left, right, broadcastJoinThreshold)
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
  def innerJoin(left: RDDJoinParam, right: RDDJoinParam, broadcastJoinThreshold: Long): RDD[Row] = {
    // Estimated size in bytes used to determine whether or not to use a broadcast join
    val leftSizeInBytes = left.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val rightSizeInBytes = right.estimatedSizeInBytes.getOrElse(Long.MaxValue)

    val innerJoinedRDD = if (leftSizeInBytes < broadcastJoinThreshold || rightSizeInBytes < broadcastJoinThreshold) {
      broadcastInnerJoin(left, right, broadcastJoinThreshold)
    }
    else {
      left.rdd.join(right.rdd)
    }
    innerJoinedRDD.map {
      case (key, (leftValues, rightValues)) => {
        Row.fromSeq(leftValues ++ rightValues)
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
  def fullOuterJoin(left: RDDJoinParam, right: RDDJoinParam): RDD[Row] = {
    Spark.fullOuterJoin(left.rdd, right.rdd).map {
      case (_, outerJoinResult) => {
        outerJoinResult match {
          case (Some(leftValues), Some(rightValues)) => {
            Row.fromSeq(leftValues ++ rightValues)
          }
          case (Some(leftValues), None) => {
            Row.fromSeq(leftValues ++ (1 to right.columnCount).map(i => null))
          }
          case (None, Some(rightValues)) => {
            Row.fromSeq((1 to left.columnCount).map(i => null) ++ rightValues)
          }
        }
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
  def rightOuterJoin(left: RDDJoinParam, right: RDDJoinParam, broadcastJoinThreshold: Long): RDD[Row] = {
    // Estimated size in bytes used to determine whether or not to use a broadcast join
    val leftSizeInBytes = left.estimatedSizeInBytes.getOrElse(Long.MaxValue)

    val rightJoinedRDD = if (leftSizeInBytes < broadcastJoinThreshold) {
      broadcastRightOuterJoin(left, right)
    }
    else {
      left.rdd.rightOuterJoin(right.rdd)
    }
    rightJoinedRDD.map {
      case (_, (leftValues, rightValues)) => {
        leftValues match {
          case s: Some[Row] => Row.fromSeq(s.get ++ rightValues)
          case None => Row.fromSeq((1 to left.columnCount).map(i => null) ++ rightValues)
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
  def leftOuterJoin(left: RDDJoinParam, right: RDDJoinParam, broadcastJoinThreshold: Long): RDD[Row] = {
    val rightSizeInBytes = right.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val leftJoinedRDD = if (rightSizeInBytes < broadcastJoinThreshold) {
      broadcastLeftOuterJoin(left, right)
    }
    else {
      left.rdd.leftOuterJoin(right.rdd)
    }
    leftJoinedRDD.map {
      case (_, (leftValues, rightValues)) => {
        rightValues match {
          case s: Some[Row] => Row.fromSeq(leftValues ++ s.get)
          case None => Row.fromSeq(leftValues ++ (1 to right.columnCount).map(i => null))
        }
      }
    }
  }

  /**
   * Perform left outer-join using a broadcast variable
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   *
   * @return key-value RDD whose values are results of left-outer join
   */
  def broadcastLeftOuterJoin(left: RDDJoinParam, right: RDDJoinParam): RDD[(Any, (Row, Option[Row]))] = {
    val sparkContext = left.rdd.sparkContext
    //Use multi-map to handle duplicate keys
    val rightMultiMap = listToMultiMap(right.rdd.collect().toList)
    val broadcastRightMultiMap = sparkContext.broadcast(rightMultiMap)

    left.rdd.flatMap {
      case (leftKey, leftValues) => {
        broadcastRightMultiMap.value.get(leftKey) match {
          case Some(rightValueSet) => for (values <- rightValueSet) yield (leftKey, (leftValues, Some(values)))
          case _ => List((leftKey, (leftValues, None))).asInstanceOf[List[(Any, (Row, Option[Row]))]]
        }
      }
    }
  }

  /**
   * Right outer-join using a broadcast variable
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   *
   * @return key-value RDD whose values are results of right-outer join
   */
  def broadcastRightOuterJoin(left: RDDJoinParam, right: RDDJoinParam): RDD[(Any, (Option[Row], Row))] = {
    val sparkContext = left.rdd.sparkContext
    //Use multi-map to handle duplicate keys
    val leftMultiMap = listToMultiMap(left.rdd.collect().toList)
    val broadcastLeftMultiMap = sparkContext.broadcast(leftMultiMap)

    right.rdd.flatMap {
      case (rightKey, rightValues) => {
        broadcastLeftMultiMap.value.get(rightKey) match {
          case Some(leftValueSet) => for (values <- leftValueSet) yield (rightKey, (Some(values), rightValues))
          case _ => List((rightKey, (None, rightValues))).asInstanceOf[List[(Any, (Option[Row], Row))]]
        }
      }
    }
  }

  /**
   * Inner-join using a broadcast variable
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   *
   * @return key-value RDD whose values are results of inner-outer join
   */
  def broadcastInnerJoin(left: RDDJoinParam, right: RDDJoinParam, broadcastJoinThreshold: Long): RDD[(Any, (Row, Row))] = {
    val sparkContext = left.rdd.sparkContext

    val leftSizeInBytes = left.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val rightSizeInBytes = right.estimatedSizeInBytes.getOrElse(Long.MaxValue)

    val innerJoinedRDD = if (rightSizeInBytes < broadcastJoinThreshold) {
      //Use multi-map to handle duplicate keys
      val rightMultiMap = listToMultiMap(right.rdd.collect().toList)
      val broadcastRightMultiMap = sparkContext.broadcast(rightMultiMap)
      left.rdd.flatMap {
        case (leftKey, leftValues) => {
          val rightValueList = broadcastRightMultiMap.value.get(leftKey).toList
          rightValueList.flatMap(rightValues => {
            for (values <- rightValues) yield (leftKey, (leftValues, values))
          })
        }
      }
    }
    else if (leftSizeInBytes < broadcastJoinThreshold) {
      //Use multi-map to handle duplicate keys
      val leftMultiMap = listToMultiMap(right.rdd.collect().toList)
      val broadcastLeftMultiMap = sparkContext.broadcast(leftMultiMap)
      right.rdd.flatMap {
        case (rightKey, rightValues) => {
          val leftValueList = broadcastLeftMultiMap.value.get(rightKey).toList
          leftValueList.flatMap(leftValues => {
            for (values <- leftValues) yield (rightKey, (values, rightValues))
          })
        }
      }
    }
    else throw new IllegalArgumentException(s"Frame size exceeds broadcast-join-threshold: ${broadcastJoinThreshold}.")
    innerJoinedRDD
  }

  //Converts list to multi-map
  //Broadcast variables are stored as multi-maps to ensure results are not lost when RDD has duplicate keys
  private def listToMultiMap[A, B](list: List[(A, B)]) =
    list.foldLeft(new HashMap[A, Set[B]] with MultiMap[A, B]) { (acc, pair) => acc.addBinding(pair._1, pair._2) }

}