package com.intel.intelanalytics.engine.spark.statistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.frame.{ ColumnFullStatisticsReturn, ColumnSummaryStatisticsReturn }

/**
 * Statistics calculator for weighted numerical data.
 *
 * Formulas for statistics are expected to adhere to the DEFAULT formulas used by SAS in
 * http://support.sas.com/documentation/cdl/en/procstat/63104/HTML/default/viewer.htm#procstat_univariate_sect026.htm
 *
 * @param dataWeightPairs RDD of pairs of  the form (data, weight)
 */
class NumericalStatistics(dataWeightPairs: RDD[(Double, Double)]) extends Serializable {

  /*
   * TODO: TRIB-3134  Investigate one-pass algorithms for weighted skewness and kurtosis. (Currently these parameters
   *  are handled in the second pass statistics, and this accounts for our separation of summary and full statistics
   *  at the API level.)
   */

  private lazy val singlePassStatistics: FirstPassStatistics = generateSinglePassStatistics()

  private lazy val secondPassStatistics: SecondPassStatistics = generateSecondPassStatistics()

  private val distributionUtils = new DistributionUtils[Double]

  /**
   * The weighted mean of the data.
   */
  lazy val weightedMean: Double = singlePassStatistics.mean

  /**
   * The weighted geometric mean of the data. NaN when a data element is <= 0.
   */
  lazy val weightedGeometricMean: Double = if (singlePassStatistics.totalWeight > 0)
    Math.exp(singlePassStatistics.weightedSumOfLogs / singlePassStatistics.totalWeight)
  else if (singlePassStatistics.weightedSumOfLogs equals Double.NaN) {
    Double.NaN
  }
  else {
    1.toDouble
  }

  /**
   * The weighted variance of the data. NaN when there are <=1 data elements.
   */
  lazy val weightedVariance: Double = {
    val n = singlePassStatistics.count
    if (n > 1) singlePassStatistics.weightedSumOfSquaredDistancesFromMean / (n - 1) else Double.NaN
  }

  /**
   * The weighted standard deviation of the data. NaN when there are <=1 data elements of nonzero weight.
   */
  lazy val weightedStandardDeviation: Double = Math.sqrt(weightedVariance)

  /**
   * The weighted mode of the data. NaN when there are no data elements of nonzero weight.
   */
  lazy val weightedMode: Double = singlePassStatistics.mode

  /**
   * The minimum value of the data. Positive infinity when there are no data elements of nonzero weight.
   */
  lazy val min: Double = singlePassStatistics.minimum

  /**
   * The maximum value of the data. Negative infinity when there are no data elements of nonzero weight.
   */
  lazy val max: Double = singlePassStatistics.maximum

  /**
   * The number of elements in the data set of nonzero weight.
   */
  lazy val count: Long = singlePassStatistics.count

  /**
   * The lower limit of the 95% confidence interval about the mean. (Assumes that the distribution is normal.)
   * NaN when there are <= 1 data elements of nonzero weight.
   */
  lazy val meanConfidenceLower: Double =
    if (count > 1) weightedMean - (1.96) * (weightedStandardDeviation / Math.sqrt(count)) else Double.NaN

  /**
   * The lower limit of the 95% confidence interval about the mean. (Assumes that the distribution is normal.)
   * NaN when there are <= 1 data elements of nonzero weight.
   */
  lazy val meanConfidenceUpper: Double =
    if (count > 1) weightedMean + (1.96) * (weightedStandardDeviation / Math.sqrt(count)) else Double.NaN

  /**
   * The weighted skewness of the dataset.
   * NaN when there are <= 2 data elements of nonzero weight.
   */
  lazy val weightedSkewness: Double = {
    val n = singlePassStatistics.count

    if (n > 2) (n.toDouble / ((n - 1) * (n - 2)).toDouble) * secondPassStatistics.sumOfThirdWeighted else Double.NaN
  }

  /**
   * The weighted kurtosis of the dataset. NaN when there are <= 3 data elements of nonzero weight.
   */
  lazy val weightedKurtosis: Double = {
    val n = singlePassStatistics.count
    if (n > 3) {
      val leadingCoefficient = (n * (n + 1)).toDouble / ((n - 1) * (n - 2) * (n - 3)).toDouble

      val subtrahend = (3 * (n - 1) * (n - 1)).toDouble / ((n - 2) * (n - 3)).toDouble

      (leadingCoefficient * secondPassStatistics.sumOfFourthWeighted) - subtrahend
    }
    else {
      Double.NaN
    }
  }

  private def generateSinglePassStatistics(): FirstPassStatistics = {

    val accumulatorParam = new FirstPassStatisticsAccumulatorParam()

    val initialValue = new FirstPassStatistics(mean = 0,
      weightedSumOfSquares = 0,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = 0,
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      mode = Double.NaN,
      weightAtMode = 0,
      totalWeight = 0,
      count = 0)

    val accumulator = dataWeightPairs.sparkContext.accumulator[FirstPassStatistics](initialValue)(accumulatorParam)

    dataWeightPairs.filter(distributionUtils.hasPositiveWeight).
      map(convertDataWeightPairToFirstPassStats).foreach(x => accumulator.add(x))

    accumulator.value
  }

  private def generateSecondPassStatistics(): SecondPassStatistics = {

    val mean = weightedMean
    val stddev = weightedStandardDeviation

    if (stddev != 0) {
      val accumulatorParam = new SecondPassStatisticsAccumulatorParam()
      val initialValue = new SecondPassStatistics(0, 0)
      val accumulator = dataWeightPairs.sparkContext.accumulator[SecondPassStatistics](initialValue)(accumulatorParam)

      dataWeightPairs.filter(distributionUtils.hasPositiveWeight).
        map(x => convertDataWeightPairToSecondPassStats(x, mean, stddev)).foreach(x => accumulator.add(x))

      accumulator.value
    } else {
      SecondPassStatistics(Double.NaN, Double.NaN)
    }


  }

  private def convertDataWeightPairToFirstPassStats(p: (Double, Double)): FirstPassStatistics = {
    val data = p._1
    val weight = p._2

    val weightedLog = if (data <= 0) Double.NaN else weight * Math.log(data)

    FirstPassStatistics(mean = data,
      weightedSumOfSquares = weight * data * data,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = weightedLog,
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

    val thirdWeighted = if (stddev != 0) Math.pow(w, 1.5) * Math.pow((x - mean) / stddev, 3) else Double.NaN
    val fourthWeighted =if (stddev != 0) Math.pow(w, 2) * Math.pow((x - mean) / stddev, 4) else Double.NaN
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
private case class FirstPassStatistics(mean: Double,
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
private class FirstPassStatisticsAccumulatorParam extends AccumulatorParam[FirstPassStatistics] with Serializable {

  override def zero(initialValue: FirstPassStatistics) =
    FirstPassStatistics(mean = 0,
      weightedSumOfSquares = 0,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = 0,
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      mode = Double.NaN,
      weightAtMode = 0,
      totalWeight = 0,
      count = 0)

  override def addInPlace(stats1: FirstPassStatistics, stats2: FirstPassStatistics): FirstPassStatistics = {

    val totalWeight = stats1.totalWeight + stats2.totalWeight
    val mean =
      if (totalWeight > 0) (stats1.mean * stats1.totalWeight + stats2.mean * stats2.totalWeight) / totalWeight else 0

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
 * In the notation of:
 * http://support.sas.com/documentation/cdl/en/procstat/63104/HTML/default/viewer.htm#procstat_univariate_sect026.htm
 *
 * xw : the weighted mean of the data
 * sw: the weighted standard deviation of the data
 *
 * @param sumOfThirdWeighted  Sum over data-weight pairs (x, w) of  Math.pow(w, 1.5) * Math.pow((x - xw) / sw, 3)
 * @param sumOfFourthWeighted Sum over data-weight pairs (x, w) of Math.pow(w, 2) * Math.pow(((x - xw) / sw), 4) })
 */
private case class SecondPassStatistics(sumOfThirdWeighted: Double, sumOfFourthWeighted: Double) extends Serializable

private class SecondPassStatisticsAccumulatorParam() extends AccumulatorParam[SecondPassStatistics] with Serializable {

  override def zero(initialValue: SecondPassStatistics) = SecondPassStatistics(0, 0)

  override def addInPlace(stats1: SecondPassStatistics, stats2: SecondPassStatistics): SecondPassStatistics = {

    val sumOfThirdWeighted = stats1.sumOfThirdWeighted + stats2.sumOfThirdWeighted

    val sumOfFourthWeighted = stats1.sumOfFourthWeighted + stats2.sumOfFourthWeighted

    SecondPassStatistics(sumOfThirdWeighted = sumOfThirdWeighted, sumOfFourthWeighted = sumOfFourthWeighted)
  }
}
