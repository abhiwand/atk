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

import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows._
import org.apache.spark.SparkException
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Functions for computing various types of cumulative distributions
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
private[spark] object CumulativeDistFunctions extends Serializable {

  /**
   * Generate the empirical cumulative distribution for an input dataframe column
   *
   * @param frameRdd rdd for a BigFrame
   * @param sampleIndex index of the column containing the sample data
   * @param dataType the data type of the input column
   * @return a new RDD of tuples containing each distinct sample value and its ecdf value
   */
  def ecdf(frameRdd: RDD[Row], sampleIndex: Int, dataType: String): RDD[Row] = {
    // parse values
    val pairedRdd = try {
      frameRdd.map(row => (java.lang.Double.parseDouble(row(sampleIndex).toString), java.lang.Double.parseDouble(row(sampleIndex).toString)))
    }
    catch {
      case cce: NumberFormatException => throw new NumberFormatException("Non-numeric column: " + cce.toString)
    }

    // get sample size
    val numValues = pairedRdd.count().toDouble

    // group identical values together
    val groupedRdd = pairedRdd.groupByKey()

    // count number of each distinct value and sort by distinct value
    val sortedRdd = groupedRdd.map { case (value, seqOfValue) => (value, seqOfValue.size) }.sortByKey()

    // compute the partition sums
    val partSums: Array[Double] = 0.0 +: sortedRdd.mapPartitionsWithIndex {
      case (index, partition) => Iterator(partition.map(pair => pair._2).sum.toDouble)
    }.collect()

    // compute empirical cumulative distribution
    val sumsRdd = sortedRdd.mapPartitionsWithIndex {
      case (index, partition) => {
        var startValue = 0.0
        for (i <- 0 to index) {
          startValue += partSums(i)
        }
        partition.scanLeft((0.0, startValue))((prev, curr) => (curr._1, prev._2 + curr._2)).drop(1)
      }
    }

    sumsRdd.map {
      case (value, valueSum) => {
        dataType match {
          case "int32" => Array(value.toInt.asInstanceOf[Any], (valueSum / numValues).asInstanceOf[Any])
          case "int64" => Array(value.toLong.asInstanceOf[Any], (valueSum / numValues).asInstanceOf[Any])
          case "float32" => Array(value.toFloat.asInstanceOf[Any], (valueSum / numValues).asInstanceOf[Any])
          case "float64" => Array(value.toDouble.asInstanceOf[Any], (valueSum / numValues).asInstanceOf[Any])
          case _ => Array(value.asInstanceOf[Any], (valueSum / numValues).asInstanceOf[Any])
        }
      }
    }
  }

  /**
   * Compute the cumulative sum of the input frameRdd for the specified column index
   *
   * @param frameRdd input frame RDD
   * @param sampleIndex index of the column to compute cumulative sum
   * @return an RDD of tuples containing (originalValue, cumulativeSumAtThisValue)
   */
  def cumulativeSum(frameRdd: RDD[Row], sampleIndex: Int): RDD[Row] = {
    // parse values
    val pairedRdd = try {
      frameRdd.map(row => (row, DataTypes.toDouble(row(sampleIndex))))
    }
    catch {
      case se: SparkException => throw new SparkException("Non-numeric column: " + se.toString)
      case cce: NumberFormatException => throw new NumberFormatException("Non-numeric column: " + cce.toString)
    }

    // compute the partition sums
    val partSums = partitionSums(pairedRdd)

    // compute cumulative sum
    val cumulativeSums = totalPartitionSums(pairedRdd, partSums)

    revertTypes(cumulativeSums)
  }

  /**
   * Compute the cumulative count of the input frameRdd for the specified column index
   *
   * @param frameRdd input frame RDD
   * @param sampleIndex index of the column to compute cumulative count
   * @param countValue the value to count
   * @return an RDD of tuples containing (originalValue, cumulativeCountAtThisValue)
   */
  def cumulativeCount(frameRdd: RDD[Row], sampleIndex: Int, countValue: String): RDD[Row] = {
    // parse values
    val pairedRdd = frameRdd.map(row => {
      val sampleValue = row(sampleIndex).toString
      if (sampleValue.equals(countValue)) {
        (row, 1.0)
      }
      else {
        (row, 0.0)
      }
    })

    // compute the partition sums
    val partSums = partitionSums(pairedRdd)

    // compute cumulative count
    val cumulativeCounts = totalPartitionSums(pairedRdd, partSums)

    revertTypes(cumulativeCounts)
  }

  /**
   * Compute the cumulative percent sum of the input frameRdd for the specified column index
   *
   * @param frameRdd input frame RDD
   * @param sampleIndex index of the column to compute cumulative percent sum
   * @return an RDD of tuples containing (originalValue, cumulativePercentSumAtThisValue)
   */
  def cumulativePercentSum(frameRdd: RDD[Row], sampleIndex: Int): RDD[Row] = {
    // parse values
    val pairedRdd = try {
      frameRdd.map(row => (row, DataTypes.toDouble(row(sampleIndex))))
    }
    catch {
      case cce: NumberFormatException => throw new NumberFormatException("Non-numeric column: " + cce.toString)
    }

    // compute the partition sums
    val partSums = partitionSums(pairedRdd)

    val numValues = pairedRdd.map { case (row, columnValue) => columnValue }.sum()

    // compute cumulative sum
    val cumulativeSums = totalPartitionSums(pairedRdd, partSums)

    revertPercentTypes(cumulativeSums, numValues)
  }

  /**
   * Compute the cumulative percent count of the input frameRdd for the specified column index
   *
   * @param frameRdd input frame RDD
   * @param sampleIndex index of the column to compute cumulative percent count
   * @param countValue the value to count
   * @return an RDD of tuples containing (originalValue, cumulativePercentCountAtThisValue)
   */
  def cumulativePercentCount(frameRdd: RDD[Row], sampleIndex: Int, countValue: String): RDD[Row] = {
    // parse values
    val pairedRdd = frameRdd.map(row => {
      val sampleValue = row(sampleIndex).toString
      if (sampleValue.equals(countValue)) {
        (row, 1.0)
      }
      else {
        (row, 0.0)
      }
    })

    // compute the partition sums
    val partSums = partitionSums(pairedRdd)

    val numValues = pairedRdd.map { case (row, columnValue) => columnValue }.sum()

    // compute cumulative count
    val cumulativeCounts = totalPartitionSums(pairedRdd, partSums)

    revertPercentTypes(cumulativeCounts, numValues)
  }

  /**
   * Compute the sum for each partition in RDD
   *
   * @param rdd the input RDD
   * @return an Array[Double] that contains the partition sums
   */
  private def partitionSums(rdd: RDD[(Row, Double)]): Array[Double] = {
    0.0 +: rdd.mapPartitionsWithIndex {
      case (index, partition) => Iterator(partition.map { case (row, columnValue) => columnValue }.sum)
    }.collect()
  }

  /**
   * Compute the total sums across partitions
   *
   * @param rdd the input RDD
   * @param partSums the sums for each partition
   * @return RDD of (value, cumulativeSum)
   */
  private def totalPartitionSums(rdd: RDD[(Row, Double)], partSums: Array[Double]): RDD[(Row, Double)] = {
    rdd.mapPartitionsWithIndex {
      case (index, partition) => {
        var startValue = 0.0
        for (i <- 0 to index) {
          startValue += partSums(i)
        }
        // startValue updated, so drop first value
        partition.scanLeft((Array[Any](), startValue))((prev, curr) => (curr._1, prev._2 + curr._2)).drop(1)
      }
    }
  }

  /**
   * Casts the input data back to the original input type
   *
   * @param rdd the RDD containing (value, cumulativeDistValue)
   * @return RDD containing Array[Any] (i.e., Rows)
   */
  private def revertTypes(rdd: RDD[(Row, Double)]): RDD[Array[Any]] = {
    rdd.map {
      case (row, valueSum) => {
        row.asInstanceOf[Array[Any]] :+ valueSum.asInstanceOf[Any]
      }
    }
  }

  /**
   * Casts the input data for cumulative percents back to the original input type.  This includes check for
   * divide-by-zero error.
   *
   * @param rdd the RDD containing (value, cumulativeDistValue)
   * @param numValues number of values in the user-specified column
   * @return RDD containing Array[Any] (i.e., Rows)
   */
  private def revertPercentTypes(rdd: RDD[(Row, Double)], numValues: Double): RDD[Array[Any]] = {
    rdd.map {
      case (row, valueSum) => {
        numValues match {
          case 0 => row.asInstanceOf[Array[Any]] :+ 1.asInstanceOf[Any]
          case _ => row.asInstanceOf[Array[Any]] :+ (valueSum / numValues).asInstanceOf[Any]
        }
      }
    }
  }

}
