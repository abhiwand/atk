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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.reflect.ClassTag

/**
 * Calculate order statistics for any weighted RDD of data that provides an ordering function.
 * @param dataWeightPairs RDD of (data, weight) pairs.
 * @param ordering Ordering on the data items.
 * @tparam T The type of the data objects. It must have an ordering function in scope.
 */
class OrderStatistics[T: ClassTag](dataWeightPairs: RDD[(T, Double)])(implicit ordering: Ordering[T])
    extends Serializable {

  /**
   * Option containing the median of the input distribution. The median is the least value X in the range of the
   * distribution so that the cumulative weight strictly below X is < 1/2  the total weight and the cumulative
   * distribution up to and including X is >= 1/2 the total weight.
   *
   * Values with non-positive weights are thrown out before the calculation is performed.
   * The option None is returned when the total weight is 0.
   */
  lazy val medianOption: Option[T] = computeMedian

  /*
   * Computes the median via a sort and scan approach, although the nature of RDDs greatly complicates the "simple scan"
   *
   * TODO: investigate the use of a sort-free (aka "linear time") median algorithm, TRIB-3151
   */
  private def computeMedian: Option[T] = {

    val sortedDataWeightPairs: RDD[(T, BigDecimal)] =
      dataWeightPairs.filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) }).
        map({ case (data, weight) => (data, BigDecimal(weight)) }).sortByKey(ascending = true)

    val weightsOfPartitions: Array[BigDecimal] = sortedDataWeightPairs.mapPartitions(sumWeightsInPartition).collect()

    val totalWeight: BigDecimal = weightsOfPartitions.sum

    if (totalWeight <= 0) {
      None
    }
    else {

      // the "median partition" is the partition the contains the median
      val (indexOfMedianPartition, weightInPrecedingPartitions) = findMedianPartition(weightsOfPartitions, totalWeight)

      val median: T = sortedDataWeightPairs.mapPartitionsWithIndex({
        case (partitionIndex, iterator) =>
          if (partitionIndex != indexOfMedianPartition) Iterator.empty
          else medianInSingletonIterator[T](iterator, totalWeight, weightInPrecedingPartitions)
      }).collect().head

      Some(median)
    }
  }

  // Sums the weights in an individual partition of an RDD[(T, BigDecimal)] where second coordinate is "weight'
  private def sumWeightsInPartition(it: Iterator[(T, BigDecimal)]): Iterator[BigDecimal] =
    if (it.nonEmpty) Iterator(it.map({ case (x, w) => w }).sum) else Iterator(0)

  // take an iterator for the partition that contains the median, and returns the median...
  // as the single element in a new iterator
  private def medianInSingletonIterator[T](it: Iterator[(T, BigDecimal)],
                                           totalWeight: BigDecimal,
                                           weightInPrecedingPartitions: BigDecimal): Iterator[T] = {

    val weightPrecedingMedian = (totalWeight / 2) - weightInPrecedingPartitions

    if (it.nonEmpty) {
      var currentDataWeightPair: (T, BigDecimal) = it.next
      var weightSoFar: BigDecimal = currentDataWeightPair._2

      while (weightSoFar < weightPrecedingMedian) {
        currentDataWeightPair = it.next()
        weightSoFar += currentDataWeightPair._2
      }
      Iterator(currentDataWeightPair._1)
    }
    else {
      Iterator.empty
    }
  }

  // Find the index of the partition that contains the median of the of the dataset, as well as the net weight of the
  // partitions that precede it.
  private def findMedianPartition(weightsOfPartitions: Array[BigDecimal], totalWeight: BigDecimal): (Int, BigDecimal) = {
    var currentPartition: Int = 0
    var weightInPrecedingPartitions: BigDecimal = 0
    while (weightInPrecedingPartitions + weightsOfPartitions(currentPartition) < totalWeight / 2) {
      weightInPrecedingPartitions += weightsOfPartitions(currentPartition)
      currentPartition += 1
    }
    (currentPartition, weightInPrecedingPartitions)
  }

}
