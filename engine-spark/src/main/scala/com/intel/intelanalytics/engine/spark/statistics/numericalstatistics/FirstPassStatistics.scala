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
 * @param count
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
                                                            count: Long)

private[numericalstatistics] object FirstPassStatistics {

  private val distributionUtils = new DistributionUtils[Double]

  private def convertDataWeightPairToFirstPassStats(p: (Double, Double)): FirstPassStatistics = {
    val dataAsDouble: Double = p._1
    val weightAsDouble: Double = p._2

    val dataAsBigDecimal: BigDecimal = BigDecimal(dataAsDouble)
    val weightAsBigDecimal: BigDecimal = BigDecimal(weightAsDouble)

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
      count = 1.toLong)
  }

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
      count = 0)

    val accumulator = dataWeightPairs.sparkContext.accumulator[FirstPassStatistics](initialValue)(accumulatorParam)

    dataWeightPairs.filter(distributionUtils.hasPositiveWeight).
      map(FirstPassStatistics.convertDataWeightPairToFirstPassStats).foreach(x => accumulator.add(x))

    accumulator.value
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
        count = 0)

    override def addInPlace(stats1: FirstPassStatistics, stats2: FirstPassStatistics): FirstPassStatistics = {

      val totalWeight = stats1.totalWeight + stats2.totalWeight
      val mean =
        if (totalWeight > 0)
          (stats1.mean * stats1.totalWeight + stats2.mean * stats2.totalWeight) / totalWeight
        else BigDecimal(0)

      val weightedSumOfSquares = stats1.weightedSumOfSquares + stats2.weightedSumOfSquares

      val sumOfSquaredDistancesFromMean = weightedSumOfSquares - 2 * mean * mean * totalWeight + mean * mean * totalWeight

      val weightedSumOfLogs: Option[BigDecimal] =
        if (stats1.weightedSumOfLogs.nonEmpty && stats2.weightedSumOfLogs.nonEmpty) {
          Some(stats1.weightedSumOfLogs.get + stats2.weightedSumOfLogs.get)
        }
        else {
          None
        }

      val min = Math.min(stats1.minimum, stats2.minimum)
      val max = Math.max(stats1.maximum, stats2.maximum)

      val count = stats1.count + stats2.count
      val (mode, weightAtMode) = if (stats1.weightAtMode > stats2.weightAtMode)
        (stats1.mode, stats1.weightAtMode)
      else
        (stats2.mode, stats2.weightAtMode)

      FirstPassStatistics(mean = mean,
        weightedSumOfSquares = weightedSumOfSquares,
        weightedSumOfSquaredDistancesFromMean = sumOfSquaredDistancesFromMean,
        weightedSumOfLogs = weightedSumOfLogs,
        minimum = min,
        maximum = max,
        mode = mode,
        weightAtMode = weightAtMode, totalWeight = totalWeight,
        count = count)
    }
  }
}

