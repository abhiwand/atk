package com.intel.intelanalytics.engine.spark

import org.apache.spark.{ AccumulatorParam, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.frame.{ ColumnFullStatisticsReturn, ColumnSummaryStatisticsReturn }

class NumericalStatistics(dataWeightPairs: RDD[(Double, Double)]) extends Serializable {

  lazy val summaryStatistics: ColumnSummaryStatisticsReturn = ColumnSummaryStatisticsReturn(mean = weightedMean,
    geometric_mean = weightedGeometricMean, variance = weightedVariance, standard_deviation = weightedStandardDeviation,
    mode = weightedMode, minimum = min, maximum = max, count = count)

  lazy val fullStatistics: ColumnFullStatisticsReturn = ColumnFullStatisticsReturn(mean = weightedMean,
    geometric_mean = weightedGeometricMean, variance = weightedVariance, standard_deviation = weightedStandardDeviation,
    skewness = weightedSkewness, kurtosis = weightedKurtosis, mode = weightedMode, minimum = min, maximum = max,
    count = count)

  private lazy val singlePassStatistics: SinglePassStatistics = generateSinglePassStatistics(dataWeightPairs)

  private lazy val weightedMean: Double = singlePassStatistics.weightedSum / singlePassStatistics.totalWeight

  private lazy val weightedGeometricMean: Double = Math.pow(singlePassStatistics.weightedProduct, 1 / singlePassStatistics.totalWeight)

  private lazy val weightedVariance: Double = generateVariance()

  private lazy val weightedStandardDeviation: Double = Math.sqrt(weightedVariance)

  private lazy val weightedMode: Double = singlePassStatistics.mode

  private lazy val min: Double = singlePassStatistics.minimum

  private lazy val max: Double = singlePassStatistics.maximum

  private lazy val count: Long = singlePassStatistics.count

  private lazy val weightedSkewness: Double = generateSkewness()

  private lazy val weightedKurtosis: Double = generateKurtosis()

  private def convertWeightedPairToStats(p: (Double, Double)): SinglePassStatistics = {
    val data = p._1
    val weight = p._2

    SinglePassStatistics(weightedSum = data * weight,
      weightedProduct = Math.pow(data, weight), minimum = data, maximum = data, mode = data, weightAtMode = weight,
      totalWeight = weight, count = 1.toLong)
  }

  private def generateSinglePassStatistics(dataWeightPairs: RDD[(Double, Double)]): SinglePassStatistics = {

    val accumulatorParam = new SinglePassStatisticsAccumulatorParam()
    val initialValue = new SinglePassStatistics(0, weightedProduct = 1.toDouble,
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity, 0, 0, 0, 0)
    val accumulator = dataWeightPairs.sparkContext.accumulator[SinglePassStatistics](initialValue)(accumulatorParam)

    dataWeightPairs.map(convertWeightedPairToStats).foreach(x => accumulator.add(x))

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
    require(n > 2, "Cannot calcualte skew of fewer than 3 samples")

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

case class SinglePassStatistics(weightedSum: Double, weightedProduct: Double, minimum: Double,
                                maximum: Double, mode: Double, weightAtMode: Double, totalWeight: Double, count: Long)
    extends Serializable

class SinglePassStatisticsAccumulatorParam extends AccumulatorParam[SinglePassStatistics] with Serializable {

  override def zero(initialValue: SinglePassStatistics) = SinglePassStatistics(0, weightedProduct = 1.toDouble,
    minimum = Double.PositiveInfinity,
    maximum = Double.NegativeInfinity, 0, 0, 0, 0)

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