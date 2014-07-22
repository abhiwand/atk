package com.intel.intelanalytics.engine.spark.statistics.numericalstatistics

import org.apache.spark.rdd.RDD
import org.apache.spark.AccumulatorParam
import com.intel.intelanalytics.engine.spark.statistics.DistributionUtils

/**
 * Second pass statistics - for computing higher moments about the mean.
 *
 * In the notation of:
 * http://support.sas.com/documentation/cdl/en/procstat/63104/HTML/default/viewer.htm#procstat_univariate_sect026.htm
 *
 * xw : the weighted mean of the data
 * sw: the weighted standard deviation of the data
 *
 * @param sumOfThirdWeighted  Sum over data-weight pairs (x, w) of  Math.pow(w, 1.5) * Math.pow((x - xw) / sw, 3).
 *                            Used to calculate skewness.
 * @param sumOfFourthWeighted Sum over data-weight pairs (x, w) of Math.pow(w, 2) * Math.pow(((x - xw) / sw), 4) })
 *                            Used to calculate kurtosis.
 */
private[numericalstatistics] case class SecondPassStatistics(sumOfThirdWeighted: Option[BigDecimal],
                                                             sumOfFourthWeighted: Option[BigDecimal]) extends Serializable
/*
 * TODO: TRIB-3134  Investigate one-pass algorithms for weighted skewness and kurtosis. (Currently these parameters
 *  are handled in the second pass statistics, and this accounts for our separation of summary and full statistics
 *  at the API level.)
 */

private[numericalstatistics] object SecondPassStatistics {

  private val distributionUtils = new DistributionUtils[Double]

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
      dataWeightPairs.filter(distributionUtils.isFiniteDoublePair).filter(distributionUtils.hasPositiveWeight).
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