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

import org.apache.spark.AccumulatorParam

/**
 * Accumulator param for combiningin FirstPassStatistics.
 */
private[numericalstatistics] class FirstPassStatisticsAccumulatorParam
    extends AccumulatorParam[FirstPassStatistics] with Serializable {

  override def zero(initialValue: FirstPassStatistics) =
    FirstPassStatistics(mean = 0,
      weightedSumOfSquares = 0,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = Some(0),
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      totalWeight = 0,
      positiveWeightCount = 0,
      nonPositiveWeightCount = 0,
      badRowCount = 0,
      goodRowCount = 0)

  override def addInPlace(stats1: FirstPassStatistics, stats2: FirstPassStatistics): FirstPassStatistics = {

    val totalWeight = stats1.totalWeight + stats2.totalWeight

    val mean = if (totalWeight > BigDecimal(0))
      (stats1.mean * stats1.totalWeight + stats2.mean * stats2.totalWeight) / totalWeight
    else
      BigDecimal(0)

    val weightedSumOfSquares = stats1.weightedSumOfSquares + stats2.weightedSumOfSquares

    val sumOfSquaredDistancesFromMean =
      weightedSumOfSquares - BigDecimal(2) * mean * mean * totalWeight + mean * mean * totalWeight

    val weightedSumOfLogs: Option[BigDecimal] =
      if (stats1.weightedSumOfLogs.nonEmpty && stats2.weightedSumOfLogs.nonEmpty) {
        Some(stats1.weightedSumOfLogs.get + stats2.weightedSumOfLogs.get)
      }
      else {
        None
      }

    FirstPassStatistics(mean = mean,
      weightedSumOfSquares = weightedSumOfSquares,
      weightedSumOfSquaredDistancesFromMean = sumOfSquaredDistancesFromMean,
      weightedSumOfLogs = weightedSumOfLogs,
      minimum = Math.min(stats1.minimum, stats2.minimum),
      maximum = Math.max(stats1.maximum, stats2.maximum),
      totalWeight = totalWeight,
      positiveWeightCount = stats1.positiveWeightCount + stats2.positiveWeightCount,
      nonPositiveWeightCount = stats1.nonPositiveWeightCount + stats2.nonPositiveWeightCount,
      badRowCount = stats1.badRowCount + stats2.badRowCount,
      goodRowCount = stats1.goodRowCount + stats2.goodRowCount)
  }
}
