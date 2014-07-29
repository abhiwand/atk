package com.intel.intelanalytics.engine.spark.statistics.numericalstatistics

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
      mode = Double.NaN,
      weightAtMode = 0,
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

    val (mode, weightAtMode) = if (stats1.weightAtMode > stats2.weightAtMode)
      (stats1.mode, stats1.weightAtMode)
    else if (stats1.weightAtMode < stats2.weightAtMode)
      (stats2.mode, stats2.weightAtMode)
    else if (stats1.mode <= stats2.mode)
      (stats1.mode, stats1.weightAtMode)
    else
      (stats2.mode, stats2.weightAtMode)

    FirstPassStatistics(mean = mean,
      weightedSumOfSquares = weightedSumOfSquares,
      weightedSumOfSquaredDistancesFromMean = sumOfSquaredDistancesFromMean,
      weightedSumOfLogs = weightedSumOfLogs,
      minimum = Math.min(stats1.minimum, stats2.minimum),
      maximum = Math.max(stats1.maximum, stats2.maximum),
      mode = mode,
      weightAtMode = weightAtMode, totalWeight = totalWeight,
      positiveWeightCount = stats1.positiveWeightCount + stats2.positiveWeightCount,
      nonPositiveWeightCount = stats1.nonPositiveWeightCount + stats2.nonPositiveWeightCount,
      badRowCount = stats1.badRowCount + stats2.badRowCount,
      goodRowCount = stats1.goodRowCount + stats2.goodRowCount)
  }
}