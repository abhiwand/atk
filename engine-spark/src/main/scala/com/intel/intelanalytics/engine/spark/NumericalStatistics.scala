package com.intel.intelanalytics.engine.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class SimpleStatistics(weightedSum: Double, weightedProduct: Double, weightedMin: Double,
                            weightedMax: Double, totalWeight: Double, count: Long)

class NumericalStatistics(dataWeightPairs: RDD[(Double, Double)]) {

  lazy val simpleStatistics: SimpleStatistics = generateFirstOrderStats(dataWeightPairs)

  lazy val weightedSum: Double = simpleStatistics.weightedSum

  lazy val weightedMean: Double = simpleStatistics.weightedSum / simpleStatistics.totalWeight

  lazy val weightedMin: Double = simpleStatistics.weightedMin

  lazy val weightedMax: Double = simpleStatistics.weightedMax

  lazy val weightedGeometricMean: Double = Math.pow(simpleStatistics.weightedProduct, 1 / simpleStatistics.totalWeight)

  lazy val weightedVariance: Double = generateVariance()

  lazy val weightedStandardDeviation: Double = Math.sqrt(weightedVariance)

  lazy val weightedSkewness: Double = generateSkewness()

  lazy val weightedKurtosis: Double = generateKurtosis()

  private def convertWeightedPairToStats(p: (Double, Double)): SimpleStatistics = {
    val data = p._1
    val weight = p._2

    SimpleStatistics(weightedSum = data * weight,
      weightedProduct = Math.pow(data, weight), weightedMin = data * weight, weightedMax = data * weight,
      totalWeight = weight, count = 1.toLong)
  }

  private def combineFirstOrderStats(stats1: SimpleStatistics, stats2: SimpleStatistics) = {

    val weightedSum = stats1.weightedSum + stats2.weightedSum
    val weightedProduct = stats1.weightedProduct * stats2.weightedProduct
    val weightedMin = Math.min(stats1.weightedMin, stats2.weightedMin)
    val weightedMax = Math.max(stats1.weightedMax, stats2.weightedMax)
    val totalWeight = stats1.totalWeight + stats2.totalWeight
    val count = stats1.count + stats2.count

    SimpleStatistics(weightedSum, weightedProduct, weightedMin, weightedMax, totalWeight, count)
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