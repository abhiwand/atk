package com.intel.intelanalytics.engine.spark.statistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.frame.{ ColumnFullStatisticsReturn, ColumnSummaryStatisticsReturn }

/**
 * Statistics calculator for weighted numerical data.
 *
 * TODO: TRIB-3134  Investigate one-pass algorithms for weighted skewness and kurtosis.
 * If we could do this, we could simplify this datastructure by unifying full and summary statistics.
 *
 *
 * @param dataWeightPairs RDD of pairs of  the form (data, weight)
 */
class NumericalStatistics(dataWeightPairs: RDD[(Double, Double)]) extends Serializable {

  lazy val singlePassStatistics: SinglePassStatistics = generateSinglePassStatistics()

  lazy val weightedMean: Double = singlePassStatistics.weightedSum / singlePassStatistics.totalWeight

  lazy val weightedGeometricMean: Double = Math.pow(singlePassStatistics.weightedProduct, 1 / singlePassStatistics.totalWeight)

  lazy val weightedVariance: Double = generateVariance()

  lazy val weightedStandardDeviation: Double = Math.sqrt(weightedVariance)

  lazy val weightedMode: Double = singlePassStatistics.mode

  lazy val min: Double = singlePassStatistics.minimum

  lazy val max: Double = singlePassStatistics.maximum

  lazy val count: Long = singlePassStatistics.count

  lazy val meanConfidenceLower: Double = weightedMean - (1.96) * (weightedStandardDeviation / Math.sqrt(count))

  lazy val meanConfidenceUpper: Double = weightedMean + (1.96) * (weightedStandardDeviation / Math.sqrt(count))

  lazy val weightedSkewness: Double = generateSkewness()

  lazy val weightedKurtosis: Double = generateKurtosis()

  private def convertDataWeightPairToStats(p: (Double, Double)): SinglePassStatistics = {
    val data = p._1
    val weight = p._2

    SinglePassStatistics(weightedSum = data * weight,
      weightedProduct = Math.pow(data, weight),
      minimum = data,
      maximum = data,
      mode = data,
      weightAtMode = weight,
      totalWeight = weight,
      count = 1.toLong)
  }

  private def generateSinglePassStatistics(): SinglePassStatistics = {

    val accumulatorParam = new SinglePassStatisticsAccumulatorParam()

    val initialValue = new SinglePassStatistics(0,
      weightedProduct = 1.toDouble,
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      mode = 0,
      weightAtMode = 0,
      totalWeight = 0,
      count = 0)

    val accumulator = dataWeightPairs.sparkContext.accumulator[SinglePassStatistics](initialValue)(accumulatorParam)

    dataWeightPairs.map(convertDataWeightPairToStats).foreach(x => accumulator.add(x))

    accumulator.value
  }

  private def generateVariance(): Double = {
    require(singlePassStatistics.count > 1, "Cannot compute variance of one value")

    val n = singlePassStatistics.count
    val xw = weightedMean

    (1.toDouble / (n - 1).toDouble) * dataWeightPairs.map({ case (x, w) => w * (x - xw) * (x - xw) }).reduce(_ + _)
  }

  private def generateSkewness(): Double = {
    val n = singlePassStatistics.count
    require(n > 2, "Cannot calculate skew of fewer than 3 samples")

    val xw = weightedMean

    val sw = weightedStandardDeviation

    (n.toDouble / ((n - 1) * (n - 2)).toDouble) *
      dataWeightPairs.map({
        case (x, w) =>
          Math.pow(w, 1.5) * Math.pow((x - xw) / sw, 3)
      }).reduce(_ + _)
  }

  private def generateKurtosis(): Double = {
    val n = singlePassStatistics.count
    require(n > 3, "Cannot calculate kurtosis of fewer than 4 samples")

    val xw = weightedMean

    val sw = weightedStandardDeviation

    val leadingCoefficient = (n * (n + 1)).toDouble / ((n - 1) * (n - 2) * (n - 3)).toDouble

    val theSum = dataWeightPairs.map({ case (x, w) => Math.pow(w, 2) * Math.pow(((x - xw) / sw), 4) }).reduce(_ + _)

    val subtrahend = (3 * (n - 1) * (n - 1)).toDouble / ((n - 2) * (n - 3)).toDouble

    (leadingCoefficient * theSum) - subtrahend
  }
}

/**
 * Contains all statistics that are computed in single pass over the data.
 * @param weightedSum
 * @param weightedProduct
 * @param minimum
 * @param maximum
 * @param mode
 * @param weightAtMode
 * @param totalWeight
 * @param count
 */
case class SinglePassStatistics(weightedSum: Double, weightedProduct: Double, minimum: Double,
                                maximum: Double, mode: Double, weightAtMode: Double, totalWeight: Double, count: Long)
    extends Serializable

/**
 * Accumulator settings for gathering single pass statistics.
 */
class SinglePassStatisticsAccumulatorParam extends AccumulatorParam[SinglePassStatistics] with Serializable {

  override def zero(initialValue: SinglePassStatistics) =
    SinglePassStatistics(weightedSum = 0,
      weightedProduct = 1.toDouble,
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      mode = 0,
      weightAtMode = 0,
      totalWeight = 0,
      count = 0)

  override def addInPlace(stats1: SinglePassStatistics, stats2: SinglePassStatistics): SinglePassStatistics = {

    val weightedSum = stats1.weightedSum + stats2.weightedSum
    val weightedProduct = stats1.weightedProduct * stats2.weightedProduct
    val weightedMin = Math.min(stats1.minimum, stats2.minimum)
    val weightedMax = Math.max(stats1.maximum, stats2.maximum)
    val totalWeight = stats1.totalWeight + stats2.totalWeight
    val count = stats1.count + stats2.count
    val (mode, weightAtMode) = if (stats1.weightAtMode > stats2.weightAtMode)
      (stats1.mode, stats1.weightAtMode)
    else
      (stats2.mode, stats2.weightAtMode)
    SinglePassStatistics(weightedSum, weightedProduct, weightedMin, weightedMax, mode, weightAtMode, totalWeight, count)
  }

}