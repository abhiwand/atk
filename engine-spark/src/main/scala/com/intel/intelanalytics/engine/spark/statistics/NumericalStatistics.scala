package com.intel.intelanalytics.engine.spark.statistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.frame.{ ColumnFullStatisticsReturn, ColumnSummaryStatisticsReturn }

/**
 * Statistics calculator for weighted numerical data.
 *
 * TODO: TRIB-3134  Investigate one-pass algorithms for weighted skewness and kurtosis. (Currently these parameters
 *  are handled in the second pass statistics, and this accounts for our separation of summary and full statistics
 *  at the API level.)
 *
 * Formulas for statistics are expected to adhere to the DEFAULT formulas used by SAS in
 * http://support.sas.com/documentation/cdl/en/procstat/63104/HTML/default/viewer.htm#procstat_univariate_sect026.htm
 *
 * @param dataWeightPairs RDD of pairs of  the form (data, weight)
 */
class NumericalStatistics(dataWeightPairs: RDD[(Double, Double)]) extends Serializable {

  private lazy val singlePassStatistics: FirstPassStatistics = generateSinglePassStatistics()

  private lazy val secondPassStatistics: SecondPassStatistics = generateSecondPassStatistics()

  /**
   * The weighted mean of the data.
   */
  lazy val weightedMean: Double = singlePassStatistics.mean

  /**
   * The weighted geometric mean of the data.
   */
  lazy val weightedGeometricMean: Double =
    Math.exp(singlePassStatistics.weightedSumOfLogs / singlePassStatistics.totalWeight)

  /**
   * The weighted variance of the data.
   */
  lazy val weightedVariance: Double =
    singlePassStatistics.weightedSumOfSquaredDistancesFromMean / (singlePassStatistics.count - 1)

  /**
   * The weighted standard deviation of the data.
   */
  lazy val weightedStandardDeviation: Double = Math.sqrt(weightedVariance)

  /**
   * The weighted mode of the data.
   */
  lazy val weightedMode: Double = singlePassStatistics.mode

  /**
   * The minimum value of the data.
   */
  lazy val min: Double = singlePassStatistics.minimum

  /**
   * The maximum value of the data.
   */
  lazy val max: Double = singlePassStatistics.maximum

  /**
   * The number of elements in the data set.
   */
  lazy val count: Long = singlePassStatistics.count

  /**
   * The lower limit of the 95% confidence interval about the mean. (Assumes that the distribution is normal.)
   */
  lazy val meanConfidenceLower: Double = weightedMean - (1.96) * (weightedStandardDeviation / Math.sqrt(count))

  /**
   * The lower limit of the 95% confidence interval about the mean. (Assumes that the distribution is normal.)
   */
  lazy val meanConfidenceUpper: Double = weightedMean + (1.96) * (weightedStandardDeviation / Math.sqrt(count))

  /**
   * The weighted skewness of the dataset.
   */
  lazy val weightedSkewness: Double = {
    val n = singlePassStatistics.count
    require(n > 2, "Cannot calculate skew of fewer than 3 samples")

    (n.toDouble / ((n - 1) * (n - 2)).toDouble) * secondPassStatistics.sumOfThirdWeighted
  }

  /**
   * The weighted kurtosis of the dataset.
   */
  lazy val weightedKurtosis: Double = {
    val n = singlePassStatistics.count
    require(n > 3, "Cannot calculate kurtosis of fewer than 4 samples")

    val leadingCoefficient = (n * (n + 1)).toDouble / ((n - 1) * (n - 2) * (n - 3)).toDouble

    val subtrahend = (3 * (n - 1) * (n - 1)).toDouble / ((n - 2) * (n - 3)).toDouble

    (leadingCoefficient * secondPassStatistics.sumOfFourthWeighted) - subtrahend
  }

  private def generateSinglePassStatistics(): FirstPassStatistics = {

    val accumulatorParam = new FirstPassStatisticsAccumulatorParam()

    val initialValue = new FirstPassStatistics(mean = 0,
      weightedSumOfSquares = 0,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = 0,
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      mode = 0,
      weightAtMode = 0,
      totalWeight = 0,
      count = 0)

    val accumulator = dataWeightPairs.sparkContext.accumulator[FirstPassStatistics](initialValue)(accumulatorParam)

    dataWeightPairs.map(convertDataWeightPairToFirstPassStats).foreach(x => accumulator.add(x))

    accumulator.value
  }

  private def generateSecondPassStatistics(): SecondPassStatistics = {

    val mean = weightedMean
    val stddev = weightedStandardDeviation

    val accumulatorParam = new SecondPassStatisticsAccumulatorParam()

    val initialValue = new SecondPassStatistics(0, 0)

    val accumulator = dataWeightPairs.sparkContext.accumulator[SecondPassStatistics](initialValue)(accumulatorParam)

    dataWeightPairs.map(x => convertDataWeightPairToSecondPassStats(x, mean, stddev)).foreach(x => accumulator.add(x))

    accumulator.value
  }

  private def convertDataWeightPairToFirstPassStats(p: (Double, Double)): FirstPassStatistics = {
    val data = p._1
    val weight = p._2

    FirstPassStatistics(mean = data,
      weightedSumOfSquares = weight * data * data,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = weight * Math.log(data),
      minimum = data,
      maximum = data,
      mode = data,
      weightAtMode = weight,
      totalWeight = weight,
      count = 1.toLong)
  }

  private def convertDataWeightPairToSecondPassStats(p: (Double, Double), mean: Double, stddev: Double): SecondPassStatistics = {
    val x = p._1
    val w = p._2

    val thirdWeighted = Math.pow(w, 1.5) * Math.pow((x - mean) / stddev, 3)
    val fourthWeighted = Math.pow(w, 2) * Math.pow((x - mean) / stddev, 4)
    SecondPassStatistics(sumOfThirdWeighted = thirdWeighted, sumOfFourthWeighted = fourthWeighted)
  }
}

/**
 * Contains all statistics that are computed in a single pass over the data. All statistics are in their weighted form.
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
case class FirstPassStatistics(mean: Double,
                               weightedSumOfSquares: Double,
                               weightedSumOfSquaredDistancesFromMean: Double,
                               weightedSumOfLogs: Double,
                               minimum: Double,
                               maximum: Double,
                               mode: Double,
                               weightAtMode: Double,
                               totalWeight: Double,
                               count: Long)
    extends Serializable

/**
 * Accumulator settings for gathering single pass statistics.
 */
class FirstPassStatisticsAccumulatorParam extends AccumulatorParam[FirstPassStatistics] with Serializable {

  override def zero(initialValue: FirstPassStatistics) =
    FirstPassStatistics(mean = 0,
      weightedSumOfSquares = 0,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = 0,
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      mode = 0,
      weightAtMode = 0,
      totalWeight = 0,
      count = 0)

  override def addInPlace(stats1: FirstPassStatistics, stats2: FirstPassStatistics): FirstPassStatistics = {

    val totalWeight = stats1.totalWeight + stats2.totalWeight
    val mean = (stats1.mean * stats1.totalWeight + stats2.mean * stats2.totalWeight) / totalWeight

    val weightedSumOfSquares = stats1.weightedSumOfSquares + stats2.weightedSumOfSquares

    val sumOfSquaredDistancesFromMean = weightedSumOfSquares - 2 * mean * mean * totalWeight + mean * mean * totalWeight

    val weightedSumOfLogs = stats1.weightedSumOfLogs + stats2.weightedSumOfLogs
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

/**
 * Second pass statistics - for computing higher moments from the mean.
 *
 * @param sumOfThirdWeighted  dataWeightPairs.map({case (x, w) => Math.pow(w, 1.5) * Math.pow((x - xw) / sw, 3)
 * }).reduce(_ + _)
 * @param sumOfFourthWeighted dataWeightPairs.map({ case (x, w) => Math.pow(w, 2) * Math.pow(((x - xw) / sw), 4) }).reduce(_ + _)
 */
case class SecondPassStatistics(sumOfThirdWeighted: Double, sumOfFourthWeighted: Double) extends Serializable

class SecondPassStatisticsAccumulatorParam() extends AccumulatorParam[SecondPassStatistics] with Serializable {

  override def zero(initialValue: SecondPassStatistics) = SecondPassStatistics(0, 0)

  override def addInPlace(stats1: SecondPassStatistics, stats2: SecondPassStatistics): SecondPassStatistics = {

    val sumOfThirdWeighted = stats1.sumOfThirdWeighted + stats2.sumOfThirdWeighted

    val sumOfFourthWeighted = stats1.sumOfFourthWeighted + stats2.sumOfFourthWeighted

    SecondPassStatistics(sumOfThirdWeighted = sumOfThirdWeighted, sumOfFourthWeighted = sumOfFourthWeighted)
  }

}
