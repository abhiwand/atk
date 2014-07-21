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
   * Incoming weights and data are Doubles, but internal running sums are represented as BigDecimal to improve
   * numerical stability.
   *
   * Values are recast to Doubles before being returned because we do not want to give the false impression that
   * we are improving precision. The use of BigDecimals is only to reduce accumulated rounding error while combing
   * values over many, many entries.
   */

  private lazy val singlePassStatistics: FirstPassStatistics = generateFirstPassStatistics()

  /*
   * TODO: TRIB-3134  Investigate one-pass algorithms for weighted skewness and kurtosis. (Currently these parameters
   *  are handled in the second pass statistics, and this accounts for our separation of summary and full statistics
   *  at the API level.)
   */

  private lazy val secondPassStatistics: SecondPassStatistics = generateSecondPassStatistics()

  private val distributionUtils = new DistributionUtils[Double]

  /**
   * The weighted mean of the data.
   */
  lazy val weightedMean: Double = singlePassStatistics.mean.toDouble

  /**
   * The weighted geometric mean of the data. NaN when a data element is <= 0,
   * 1 when there are no data elements of positive weight.
   */
  lazy val weightedGeometricMean: Double = {

    val totalWeight: BigDecimal = singlePassStatistics.totalWeight
    val weightedSumOfLogs: Option[BigDecimal] = singlePassStatistics.weightedSumOfLogs

    if (totalWeight > 0 && weightedSumOfLogs.nonEmpty)
      Math.exp((weightedSumOfLogs.get / totalWeight).toDouble)
    else if (totalWeight > 0 && weightedSumOfLogs.isEmpty) {
      Double.NaN
    }
    else {
      // this is the totalWeight == 0 case
      1.toDouble
    }
  }

  /**
   * The weighted variance of the data. NaN when there are <=1 data elements.
   */
  lazy val weightedVariance: Double = {
    val n: BigDecimal = BigDecimal(singlePassStatistics.count)
    if (n > 1) (singlePassStatistics.weightedSumOfSquaredDistancesFromMean / (n - 1)).toDouble else Double.NaN
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
   * The minimum value of the data. Positive infinity when there are no data elements of positive weight.
   */
  lazy val min: Double = singlePassStatistics.minimum

  /**
   * The maximum value of the data. Negative infinity when there are no data elements of positive weight.
   */
  lazy val max: Double = singlePassStatistics.maximum

  /**
   * The number of elements in the data set of nonzero weight.
   */
  lazy val count: Long = singlePassStatistics.count

  /**
   * The lower limit of the 95% confidence interval about the mean. (Assumes that the distribution is normal.)
   * NaN when there are <= 1 data elements of positive weight.
   */
  lazy val meanConfidenceLower: Double =
    if (count > 1) weightedMean - (1.96) * (weightedStandardDeviation / Math.sqrt(count)) else Double.NaN

  /**
   * The lower limit of the 95% confidence interval about the mean. (Assumes that the distribution is normal.)
   * NaN when there are <= 1 data elements of positive weight.
   */
  lazy val meanConfidenceUpper: Double =
    if (count > 1) weightedMean + (1.96) * (weightedStandardDeviation / Math.sqrt(count)) else Double.NaN

  /**
   * The weighted skewness of the dataset.
   * NaN when there are <= 2 data elements of nonzero weight.
   */
  lazy val weightedSkewness: Double = {
    val n: BigDecimal = BigDecimal(singlePassStatistics.count)
    val sumOfThirdWeighted: Option[BigDecimal] = secondPassStatistics.sumOfThirdWeighted
    if ((n > 2) && sumOfThirdWeighted.nonEmpty)
      ((n / ((n - 1) * (n - 2))) * sumOfThirdWeighted.get).toDouble
    else Double.NaN
  }

  /**
   * The weighted kurtosis of the dataset. NaN when there are <= 3 data elements of nonzero weight.
   */
  lazy val weightedKurtosis: Double = {
    val n = BigDecimal(singlePassStatistics.count)
    val sumOfFourthWeighted: Option[BigDecimal] = secondPassStatistics.sumOfFourthWeighted
    if ((n > 3) && sumOfFourthWeighted.nonEmpty) {
      val leadingCoefficient: BigDecimal = (n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3))

      val subtrahend: BigDecimal = (3 * (n - 1) * (n - 1)) / ((n - 2) * (n - 3))

      ((leadingCoefficient * secondPassStatistics.sumOfFourthWeighted.get) - subtrahend).toDouble
    }
    else {
      Double.NaN
    }
  }

  /*
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
  private case class FirstPassStatistics(mean: BigDecimal,
                                         weightedSumOfSquares: BigDecimal,
                                         weightedSumOfSquaredDistancesFromMean: BigDecimal,
                                         weightedSumOfLogs: Option[BigDecimal],
                                         minimum: Double,
                                         maximum: Double,
                                         mode: Double,
                                         weightAtMode: Double,
                                         totalWeight: BigDecimal,
                                         count: Long)
      extends Serializable

  /*
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
  private case class SecondPassStatistics(sumOfThirdWeighted: Option[BigDecimal],
                                          sumOfFourthWeighted: Option[BigDecimal]) extends Serializable

  private def generateFirstPassStatistics(): FirstPassStatistics = {

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
      map(convertDataWeightPairToFirstPassStats).foreach(x => accumulator.add(x))

    accumulator.value
  }

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

  private def generateSecondPassStatistics(): SecondPassStatistics = {

    val mean = weightedMean
    val stddev = weightedStandardDeviation

    if (stddev != 0 && !stddev.isNaN()) {
      val accumulatorParam = new SecondPassStatisticsAccumulatorParam()
      val initialValue = new SecondPassStatistics(Some(BigDecimal(0)), Some(BigDecimal(0)))
      val accumulator = dataWeightPairs.sparkContext.accumulator[SecondPassStatistics](initialValue)(accumulatorParam)

      dataWeightPairs.filter(distributionUtils.hasPositiveWeight).
        map(x => convertDataWeightPairToSecondPassStats(x, mean, stddev)).foreach(x => accumulator.add(x))

      accumulator.value
    }
    else {
      SecondPassStatistics(None, None)
    }

  }

  private def convertDataWeightPairToSecondPassStats(p: (Double, Double), mean: Double, stddev: Double): SecondPassStatistics = {

    val dataAsDouble: Double = p._1
    val weightAsDouble: Double = p._2

    val thirdWeighted: Option[BigDecimal] =
      if (stddev != 0 && !stddev.isNaN())
        Some(BigDecimal(Math.pow(weightAsDouble, 1.5) * Math.pow((dataAsDouble - mean) / stddev, 3)))
      else
        None

    val fourthWeighted: Option[BigDecimal] =
      if (stddev != 0 && !stddev.isNaN())
        Some(BigDecimal(Math.pow(weightAsDouble, 2) * Math.pow((dataAsDouble - mean) / stddev, 4)))
      else
        None

    SecondPassStatistics(sumOfThirdWeighted = thirdWeighted, sumOfFourthWeighted = fourthWeighted)
  }

  private class SecondPassStatisticsAccumulatorParam() extends AccumulatorParam[SecondPassStatistics] with Serializable {

    override def zero(initialValue: SecondPassStatistics) = SecondPassStatistics(Some(BigDecimal(0)), Some(BigDecimal(0)))

    override def addInPlace(stats1: SecondPassStatistics, stats2: SecondPassStatistics): SecondPassStatistics = {

      val sumOfThirdWeighted: Option[BigDecimal] = if (stats1.sumOfThirdWeighted.nonEmpty && stats2.sumOfThirdWeighted.nonEmpty) {
        Some(stats1.sumOfThirdWeighted.get + stats2.sumOfThirdWeighted.get)
      }
      else {
        None
      }

      val sumOfFourthWeighted: Option[BigDecimal] = if (stats1.sumOfFourthWeighted.nonEmpty && stats2.sumOfFourthWeighted.nonEmpty) {
        Some(stats1.sumOfFourthWeighted.get + stats2.sumOfFourthWeighted.get)
      }
      else {
        None
      }

      SecondPassStatistics(sumOfThirdWeighted = sumOfThirdWeighted, sumOfFourthWeighted = sumOfFourthWeighted)
    }
  }

}

