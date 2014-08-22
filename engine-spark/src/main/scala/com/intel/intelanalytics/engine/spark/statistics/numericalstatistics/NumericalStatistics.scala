package com.intel.intelanalytics.engine.spark.statistics.numericalstatistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.frame.ColumnFullStatisticsReturn

/**
 * Statistics calculator for weighted numerical data. Data elements with non-positive weights are thrown out and do
 * not affect stastics (excepting the count of entries with non-postive weights).
 *
 * @param dataWeightPairs RDD of pairs of  the form (data, weight)
 */
class NumericalStatistics(dataWeightPairs: RDD[(Double, Double)], usePopulationVariance: Boolean) extends Serializable {

  /*
   * Incoming weights and data are Doubles, but internal running sums are represented as BigDecimal to improve
   * numerical stability.
   *
   * Values are recast to Doubles before being returned because we do not want to give the false impression that
   * we are improving precision. The use of BigDecimals is only to reduce accumulated rounding error while combing
   * values over many, many entries.
   */

  private lazy val singlePassStatistics: FirstPassStatistics = StatisticsRDDFunctions.generateFirstPassStatistics(dataWeightPairs)

  /*
   * Second pass statistics are used to calculate higher moments about the mean. We can probably get away with just
   * one pass... but not till after 0.8
   * TODO: TRIB-3134  Investigate one-pass algorithms for weighted skewness and kurtosis. (Currently these parameters
   *  are handled in the second pass statistics, and this accounts for our separation of summary and full statistics
   *  at the API level.)
   */

  private lazy val secondPassStatistics: SecondPassStatistics =
    StatisticsRDDFunctions.generateSecondPassStatistics(dataWeightPairs, weightedMean, weightedStandardDeviation)

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
    val weight: BigDecimal = singlePassStatistics.totalWeight
    if (usePopulationVariance) {
      (singlePassStatistics.weightedSumOfSquaredDistancesFromMean / weight).toDouble
    }
    else {
      if (weight > 1)
        (singlePassStatistics.weightedSumOfSquaredDistancesFromMean / (weight - 1)).toDouble
      else
        Double.NaN
    }
  }

  /**
   * The weighted standard deviation of the data. NaN when there are <=1 data elements of nonzero weight.
   */
  lazy val weightedStandardDeviation: Double = Math.sqrt(weightedVariance)

  /**
   * Sum of all weights that are finite numbers  > 0.
   */
  lazy val totalWeight: Double = singlePassStatistics.totalWeight.toDouble

  /**
   * The minimum value of the data. Positive infinity when there are no data elements of positive weight.
   */
  lazy val min: Double = if (singlePassStatistics.minimum.isInfinity) Double.NaN else singlePassStatistics.minimum

  /**
   * The maximum value of the data. Negative infinity when there are no data elements of positive weight.
   */
  lazy val max: Double = if (singlePassStatistics.maximum.isInfinity) Double.NaN else singlePassStatistics.maximum

  /**
   * The number of elements in the data set with weight > 0.
   */
  lazy val positiveWeightCount: Long = singlePassStatistics.positiveWeightCount

  /**
   * The number of pairs that contained NaNs or infinite values for a data column or a weight column (if the weight column
   */
  lazy val badRowCount: Long = singlePassStatistics.badRowCount

  /**
   * The number of pairs that contained proper finite numbers for the data column and the weight column.
   */
  lazy val goodRowCount: Long = singlePassStatistics.goodRowCount

  /**
   * The number of elements in the data set with weight <= 0.
   */
  lazy val nonPositiveWeightCount: Long = singlePassStatistics.nonPositiveWeightCount

  /**
   * The lower limit of the 95% confidence interval about the mean. (Assumes that the distribution is normal.)
   * NaN when there are <= 1 data elements of positive weight.
   */
  lazy val meanConfidenceLower: Double =

    if (positiveWeightCount > 1 && weightedStandardDeviation != Double.NaN)
      weightedMean - (1.96) * (weightedStandardDeviation / Math.sqrt(positiveWeightCount))
    else
      Double.NaN

  /**
   * The lower limit of the 95% confidence interval about the mean. (Assumes that the distribution is normal.)
   * NaN when there are <= 1 data elements of positive weight.
   */
  lazy val meanConfidenceUpper: Double =
    if (positiveWeightCount > 1 && weightedStandardDeviation != Double.NaN)
      weightedMean + (1.96) * (weightedStandardDeviation / Math.sqrt(positiveWeightCount))
    else
      Double.NaN

  /**
   * The un-weighted skewness of the dataset.
   * NaN when there are <= 2 data elements of nonzero weight.
   */
  lazy val weightedSkewness: Double = {
    val n: BigDecimal = BigDecimal(singlePassStatistics.positiveWeightCount)
    val sumOfThirdWeighted: Option[BigDecimal] = secondPassStatistics.sumOfThirdWeighted
    if ((n > 2) && sumOfThirdWeighted.nonEmpty)
      ((n / ((n - 1) * (n - 2))) * sumOfThirdWeighted.get).toDouble
    else Double.NaN
  }

  /**
   * The un-weighted kurtosis of the dataset. NaN when there are <= 3 data elements of nonzero weight.
   */
  lazy val weightedKurtosis: Double = {
    val n = BigDecimal(singlePassStatistics.positiveWeightCount)
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

}

