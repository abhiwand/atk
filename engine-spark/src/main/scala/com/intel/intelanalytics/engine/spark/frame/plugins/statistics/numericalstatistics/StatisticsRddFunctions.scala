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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.numericalstatistics

import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.NumericValidationUtils
import org.apache.spark.AccumulatorParam

/**
 * Provides static methods for calculating first pass and second pass statistics given an RDD[(Double,Double)] of
 * (data, weight) pairs.
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object StatisticsRddFunctions extends Serializable {

  /**
   * Generates the first-pass statistics for a given distribution.
   * @param dataWeightPairs The (data, weight) pairs of the distribution.
   * @return The first-pass statistics of the distribution.
   */
  def generateFirstPassStatistics(dataWeightPairs: RDD[(Option[Double], Option[Double])]): FirstPassStatistics = {

    val accumulatorParam = new FirstPassStatisticsAccumulatorParam()

    val initialValue = new FirstPassStatistics(mean = 0,
      weightedSumOfSquares = 0,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = Some(BigDecimal(0)),
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      totalWeight = 0,
      positiveWeightCount = 0,
      nonPositiveWeightCount = 0,
      badRowCount = 0,
      goodRowCount = 0)

    val accumulator = dataWeightPairs.sparkContext.accumulator[FirstPassStatistics](initialValue)(accumulatorParam)

    dataWeightPairs.map(StatisticsRddFunctions.convertDataWeightPairToFirstPassStats).foreach(x => accumulator.add(x))

    accumulator.value
  }

  private def convertDataWeightPairToFirstPassStats(p: (Option[Double], Option[Double])): FirstPassStatistics = {
    (p._1, p._2) match {
      case (None, None) | (None, _) | (_, None) => firstPassStatsOfBadEntry
      case (data, weight) =>
        val dataAsDouble: Double = data.get
        val weightAsDouble: Double = weight.get

        if (!NumericValidationUtils.isFiniteNumber(dataAsDouble)
          || !NumericValidationUtils.isFiniteNumber(weightAsDouble)) {
          firstPassStatsOfBadEntry
        }
        else {
          if (weightAsDouble <= 0) {
            firstPassStatsOfGoodEntryNonPositiveWeight
          }
          else {
            val dataAsBigDecimal: BigDecimal = BigDecimal(dataAsDouble)
            val weightAsBigDecimal: BigDecimal = BigDecimal(weightAsDouble)

            val weightedLog = if (dataAsDouble <= 0) None else Some(weightAsBigDecimal * BigDecimal(Math.log(dataAsDouble)))

            FirstPassStatistics(mean = dataAsBigDecimal,
              weightedSumOfSquares = weightAsBigDecimal * dataAsBigDecimal * dataAsBigDecimal,
              weightedSumOfSquaredDistancesFromMean = BigDecimal(0),
              weightedSumOfLogs = weightedLog,
              minimum = dataAsDouble,
              maximum = dataAsDouble,
              totalWeight = weightAsBigDecimal,
              positiveWeightCount = 1,
              nonPositiveWeightCount = 0,
              badRowCount = 0,
              goodRowCount = 1)
          }
        }
    }
  }

  private val firstPassStatsOfBadEntry = FirstPassStatistics(badRowCount = 1,
    goodRowCount = 0,
    // rows with illegal doubles are discarded, so that the statistics for such an entry
    // are simply those of an empty collection
    nonPositiveWeightCount = 0,
    positiveWeightCount = 0,
    mean = BigDecimal(0),
    weightedSumOfSquares = BigDecimal(0),
    weightedSumOfSquaredDistancesFromMean = BigDecimal(0),
    weightedSumOfLogs = Some(BigDecimal(0)),
    minimum = Double.PositiveInfinity,
    maximum = Double.NegativeInfinity,
    totalWeight = BigDecimal(0))

  private val firstPassStatsOfGoodEntryNonPositiveWeight = FirstPassStatistics(
    nonPositiveWeightCount = 1,
    positiveWeightCount = 0,
    badRowCount = 0,
    goodRowCount = 1,
    // entries of non-positive weight are discarded, so that the statistics for such an entry
    // are simply those of an empty collection
    mean = BigDecimal(0),
    weightedSumOfSquares = BigDecimal(0),
    weightedSumOfSquaredDistancesFromMean = BigDecimal(0),
    weightedSumOfLogs = Some(BigDecimal(0)),
    minimum = Double.PositiveInfinity,
    maximum = Double.NegativeInfinity,
    totalWeight = BigDecimal(0))

}
