package com.intel.intelanalytics.engine.spark.statistics.numericalstatistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.statistics.DistributionUtils

/**
 * Created by nlsegerl on 7/21/14.
 */

/**
 * Contains all statistics that are computed in a single pass over the data. All statistics are in their weighted form.
 *
 * Floating point values that are running combinations over all of the data are represented as BigDecimal, whereas
 * minimum, mode and maximum are Doubles since they are simply single data points.
 * @param mean
 * @param weightedSumOfSquares
 * @param weightedSumOfSquaredDistancesFromMean
 * @param weightedSumOfLogs
 * @param minimum
 * @param maximum
 * @param mode
 * @param weightAtMode
 * @param totalWeight
 * @param totalCount
 */
private[numericalstatistics] case class FirstPassStatistics(mean: BigDecimal,
                                                            weightedSumOfSquares: BigDecimal,
                                                            weightedSumOfSquaredDistancesFromMean: BigDecimal,
                                                            weightedSumOfLogs: Option[BigDecimal],
                                                            minimum: Double,
                                                            maximum: Double,
                                                            mode: Double,
                                                            weightAtMode: Double,
                                                            totalWeight: BigDecimal,
                                                            totalCount: Long,
                                                            nonPositiveWeightCount: Long)

private[numericalstatistics] object FirstPassStatistics {

  private val distributionUtils = new DistributionUtils[Double]

  /**
   * Generates the first-pass statistics for a given distribution.
   * @param dataWeightPairs The (data, weight) pairs of the distribution.
   * @return The first-pass statistics of the distribution.
   */
  def generate(dataWeightPairs: RDD[(Double, Double)]): FirstPassStatistics = {

    val accumulatorParam = new FirstPassStatisticsAccumulatorParam()

    val initialValue = new FirstPassStatistics(mean = 0,
      weightedSumOfSquares = 0,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = Some(BigDecimal(0)),
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      mode = Double.NaN,
      weightAtMode = 0,
      totalWeight = 0,
      totalCount = 0,
      nonPositiveWeightCount = 0)

    val accumulator = dataWeightPairs.sparkContext.accumulator[FirstPassStatistics](initialValue)(accumulatorParam)

    dataWeightPairs.map(FirstPassStatistics.convertDataWeightPairToFirstPassStats).foreach(x => accumulator.add(x))

    accumulator.value
  }

  private def convertDataWeightPairToFirstPassStats(p: (Double, Double)): FirstPassStatistics = {
    val dataAsDouble: Double = p._1
    val weightAsDouble: Double = p._2

    val dataAsBigDecimal: BigDecimal = BigDecimal(dataAsDouble)
    val weightAsBigDecimal: BigDecimal = BigDecimal(weightAsDouble)

    if (weightAsDouble > 0) {
      val weightedLog = if (dataAsDouble <= 0) None else Some(weightAsBigDecimal * BigDecimal(Math.log(dataAsDouble)))

      FirstPassStatistics(mean = dataAsBigDecimal,
        weightedSumOfSquares = weightAsBigDecimal * dataAsBigDecimal * dataAsBigDecimal,
        weightedSumOfSquaredDistancesFromMean = BigDecimal(0),
        weightedSumOfLogs = weightedLog,
        minimum = dataAsDouble,
        maximum = dataAsDouble,
        mode = dataAsDouble,
        weightAtMode = weightAsDouble,
        totalWeight = weightAsBigDecimal,
        totalCount = 1.toLong,
        nonPositiveWeightCount = 0)
    }
    else {
      FirstPassStatistics(
        nonPositiveWeightCount = 1,

        // entries of non-positive weight are discarded, so that the statistics for such an entry
        // are simply those of an empty collection
        mean = BigDecimal(0),
        weightedSumOfSquares = BigDecimal(0),
        weightedSumOfSquaredDistancesFromMean = BigDecimal(0),
        weightedSumOfLogs = Some(BigDecimal(0)),
        minimum = Double.PositiveInfinity,
        maximum = Double.NegativeInfinity,
        mode = Double.NaN,
        weightAtMode = 0,
        totalWeight = BigDecimal(0),
        totalCount = 0)
    }
  }

  /*
   * Accumulator settings for gathering single pass statistics.
   */
  private class FirstPassStatisticsAccumulatorParam extends AccumulatorParam[FirstPassStatistics] with Serializable {

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
        totalCount = 0,
        nonPositiveWeightCount = 0)

    override def addInPlace(stats1: FirstPassStatistics, stats2: FirstPassStatistics): FirstPassStatistics = {

      if (stats1.totalWeight equals BigDecimal(0)) {
        FirstPassStatistics(mean = stats2.mean,
          weightedSumOfSquares = stats2.weightedSumOfSquares,
          weightedSumOfSquaredDistancesFromMean = stats2.weightedSumOfSquaredDistancesFromMean,
          weightedSumOfLogs = stats2.weightedSumOfLogs,
          minimum = stats2.minimum,
          maximum = stats2.maximum,
          mode = stats2.mode,
          weightAtMode = stats2.weightAtMode,
          totalWeight = stats2.totalWeight,
          totalCount = stats1.totalCount + stats2.totalCount,
          nonPositiveWeightCount = stats1.nonPositiveWeightCount + stats2.nonPositiveWeightCount)
      }
      else if (stats2.totalWeight equals BigDecimal(0)) {
        FirstPassStatistics(mean = stats1.mean,
          weightedSumOfSquares = stats1.weightedSumOfSquares,
          weightedSumOfSquaredDistancesFromMean = stats1.weightedSumOfSquaredDistancesFromMean,
          weightedSumOfLogs = stats1.weightedSumOfLogs,
          minimum = stats1.minimum,
          maximum = stats1.maximum,
          mode = stats1.mode,
          weightAtMode = stats1.weightAtMode,
          totalWeight = stats1.totalWeight,
          totalCount = stats1.totalCount + stats2.totalCount,
          nonPositiveWeightCount = stats1.nonPositiveWeightCount + stats2.nonPositiveWeightCount)
      }
      else {

        val totalWeight = stats1.totalWeight + stats2.totalWeight
        val mean =
          if (totalWeight > 0)
            (stats1.mean * stats1.totalWeight + stats2.mean * stats2.totalWeight) / totalWeight
          else BigDecimal(0)

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
          totalCount = stats1.totalCount + stats2.totalCount,
          nonPositiveWeightCount = stats1.nonPositiveWeightCount + stats2.nonPositiveWeightCount)
      }
    }
  }

}

