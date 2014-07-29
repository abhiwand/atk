package com.intel.intelanalytics.engine.spark.statistics.numericalstatistics

import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.statistics.NumericValidationUtils
import org.apache.spark.AccumulatorParam

/**
 * Provides static methods for calculating first pass and second pass statistics given an RDD[(Double,Double)] of
 * (data, weight) pairs.
 */
private[numericalstatistics] object StatisticsRDDFunctions {

  /*
   * TODO: TRIB-3134  Investigate one-pass algorithms for weighted skewness and kurtosis. (Currently these parameters
   *  are handled in the second pass statistics, and this accounts for our separation of summary and full statistics
   *  at the API level.)
   */

  /**
   * Generates the first-pass statistics for a given distribution.
   * @param dataWeightPairs The (data, weight) pairs of the distribution.
   * @return The first-pass statistics of the distribution.
   */
  def generateFirstPassStatistics(dataWeightPairs: RDD[(Double, Double)]): FirstPassStatistics = {

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
      positiveWeightCount = 0,
      nonPositiveWeightCount = 0,
      badRowCount = 0,
      goodRowCount = 0)

    val accumulator = dataWeightPairs.sparkContext.accumulator[FirstPassStatistics](initialValue)(accumulatorParam)

    dataWeightPairs.map(StatisticsRDDFunctions.convertDataWeightPairToFirstPassStats).foreach(x => accumulator.add(x))

    accumulator.value
  }

  private def convertDataWeightPairToFirstPassStats(p: (Double, Double)): FirstPassStatistics = {
    val dataAsDouble: Double = p._1
    val weightAsDouble: Double = p._2

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
          mode = dataAsDouble,
          weightAtMode = weightAsDouble,
          totalWeight = weightAsBigDecimal,
          positiveWeightCount = 1,
          nonPositiveWeightCount = 0,
          badRowCount = 0,
          goodRowCount = 1)
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
    mode = Double.NaN,
    weightAtMode = 0,
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
    mode = Double.NaN,
    weightAtMode = 0,
    totalWeight = BigDecimal(0))

  /**
   * Calculate the second pass statistics for a distribution -- given its mean and standard deviation.
   * @param dataWeightPairs The (data, weight) pairs of the distribution.
   * @param mean The mean of the distribution.
   * @param stddev The standard deviation of the distribution.
   * @return Second-pass statistics.
   */
  def generateSecondPassStatistics(dataWeightPairs: RDD[(Double, Double)], mean: Double, stddev: Double): SecondPassStatistics = {

    if (stddev != 0) {
      val accumulatorParam = new SecondPassStatisticsAccumulatorParam()
      val initialValue = new SecondPassStatistics(Some(BigDecimal(0)), Some(BigDecimal(0)))
      val accumulator = dataWeightPairs.sparkContext.accumulator[SecondPassStatistics](initialValue)(accumulatorParam)

      // for second pass statistics, there's no need to keep around the data elements with non-positive weight,
      // since the first pass statistics track the count of those
      dataWeightPairs.filter(NumericValidationUtils.isFiniteDoublePair).
        filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) }).
        map(x => convertDataWeightPairToSecondPassStats(x, mean, stddev)).foreach(x => accumulator.add(x))

      accumulator.value
    }
    else {
      SecondPassStatistics(None, None)
    }

  }

  private def convertDataWeightPairToSecondPassStats(p: (Double, Double), mean: Double, stddev: Double): SecondPassStatistics = {

    val data: Double = p._1
    val weight: Double = p._2

    val thirdWeighted: Option[BigDecimal] =
      if (stddev != 0 && !stddev.isNaN())
        Some(BigDecimal(Math.pow(weight, 1.5) * Math.pow((data - mean) / stddev, 3)))
      else
        None

    val fourthWeighted: Option[BigDecimal] =
      if (stddev != 0 && !stddev.isNaN())
        Some(BigDecimal(Math.pow(weight, 2) * Math.pow((data - mean) / stddev, 4)))
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
