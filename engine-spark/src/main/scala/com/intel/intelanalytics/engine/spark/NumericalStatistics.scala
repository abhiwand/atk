package com.intel.intelanalytics.engine.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.frame.ColumnSummaryStatisticsReturn

case class SimpleStatistics(weightedSum: Double, weightedProduct: Double, minimum: Double,
                            maximum: Double, mode: Double, weightAtMode: Double, totalWeight: Double, count: Long)

class NumericalStatistics(dataWeightPairs: RDD[(Double, Double)]) extends Serializable {

  lazy val simpleStatistics: SimpleStatistics = generateFirstOrderStats(dataWeightPairs)

  lazy val weightedSum: Double = simpleStatistics.weightedSum

  lazy val weightedMean: Double = simpleStatistics.weightedSum / simpleStatistics.totalWeight

  lazy val weightedGeometricMean: Double = Math.pow(simpleStatistics.weightedProduct, 1 / simpleStatistics.totalWeight)

  lazy val weightedVariance: Double = generateVariance()

  lazy val weightedStandardDeviation: Double = Math.sqrt(weightedVariance)

  lazy val weightedMode: Double = simpleStatistics.mode

  lazy val min = simpleStatistics.minimum

  lazy val max = simpleStatistics.maximum

  lazy val count = simpleStatistics.count

  lazy val summaryStatistics: ColumnSummaryStatisticsReturn = ColumnSummaryStatisticsReturn(mean = weightedMean,
    geometric_mean = weightedGeometricMean, variance = weightedVariance, standard_deviation = weightedStandardDeviation,
    mode = weightedMode, minimum = min, maximum = max, count = count)

  lazy val weightedSkewness: Double = generateSkewness()

  lazy val weightedKurtosis: Double = generateKurtosis()

  private def convertWeightedPairToStats(p: (Double, Double)): SimpleStatistics = {
    val data = p._1
    val weight = p._2

    SimpleStatistics(weightedSum = data * weight,
      weightedProduct = Math.pow(data, weight), minimum = data, maximum = data, mode = data, weightAtMode = weight,
      totalWeight = weight, count = 1.toLong)
  }

  private def combineFirstOrderStats(stats1: SimpleStatistics, stats2: SimpleStatistics) = {

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

    SimpleStatistics(weightedSum, weightedProduct, weightedMin, weightedMax, mode, weightAtMode, totalWeight, count)
  }

  private def generateFirstOrderStats(dataWeightPairs: RDD[(Double, Double)]): SimpleStatistics = {
    dataWeightPairs.map(convertWeightedPairToStats).reduce(combineFirstOrderStats)
  }

  private def generateVariance(): Double = {
    require(simpleStatistics.count > 1, "Cannot compute variance of one value")

    val n = simpleStatistics.count
    val xw = weightedMean

    (1.toDouble / (n - 1).toDouble) * dataWeightPairs.map({ case (x, w) => w * (x - xw) * (x - xw) }).reduce(_ + _)
  }

  private def generateSkewness(): Double = {
    val n = simpleStatistics.count
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
    val n = simpleStatistics.count
    require(n > 3, "Cannot calculate kurtosis of fewer than 4 samples")

    val xw = weightedMean

    val sw = weightedStandardDeviation

    val leadingCoefficient = (n * (n + 1)).toDouble / ((n - 1) * (n - 2) * (n - 3)).toDouble

    val theSum = dataWeightPairs.map({ case (x, w) => Math.pow(w, 2) * Math.pow(((x - xw) / sw), 4) }).reduce(_ + _)

    val subtrahend = (3 * (n - 1) * (n - 1)).toDouble / ((n - 2) * (n - 3)).toDouble

    (leadingCoefficient * theSum) - subtrahend
  }
}