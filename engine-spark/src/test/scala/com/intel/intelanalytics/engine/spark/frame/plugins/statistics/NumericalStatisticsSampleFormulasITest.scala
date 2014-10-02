package com.intel.intelanalytics.engine.spark.frame.plugins.statistics

import org.scalatest.Matchers
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.numericalstatistics.NumericalStatistics

class NumericalStatisticsSampleFormulasITest extends TestingSparkContextFlatSpec with Matchers {

  /**
   * Tests the distributed implementation of the statistics calculator against the formulae for sample estimates
   * of parameters.
   *
   * It tests whether or not the distributed implementations (with the accumulators and
   * whatnot) match the textbook formulas on a small data set.
   */
  trait NumericalStatisticsTestSampleFormulas {

    val epsilon = 0.000000001

    val data = List(1, 2, 3, 4, 5, 6, 7, 8, 9).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1, 9).map(x => x.toDouble)

    require(data.length > 3, "Test Data in Error: Data should have at least four elements, lest the kurtosis be trivialized.")
    require(data.length == frequencies.length, "Test Data in Error: Data length and frequencies length are mismatched")
    val netFrequencies = frequencies.reduce(_ + _)

    val inverseProbabilityWeights = frequencies.map(x => (netFrequencies.toDouble / x))
    val netIPWeights = inverseProbabilityWeights.reduce(_ + _)

    val dataFrequencyPairs: List[(Double, Double)] = data.zip(frequencies)
    val dataFrequencyRDD = sparkContext.parallelize(dataFrequencyPairs)

    val dataIPWPairs: List[(Double, Double)] = data.zip(inverseProbabilityWeights)
    val dataIPWRDD = sparkContext.parallelize(dataIPWPairs)

    val numericalStatisticsFrequencies = new NumericalStatistics(dataFrequencyRDD, false)

    val numericalStatisticsWeights = new NumericalStatistics(dataIPWRDD, false)

    val expectedMeanFrequencies: Double = dataFrequencyPairs.map({ case (x, w) => x * w }).reduce(_ + _) / netFrequencies
    val expectedMeanIPW: Double = dataIPWPairs.map({ case (x, w) => x * w }).reduce(_ + _) / netIPWeights
    val expectedMax: Double = data.reduce(Math.max(_, _))
    val expectedMin: Double = data.reduce(Math.min(_, _))
    val dataCount: Double = data.length

    val expectedGeometricMeanIPW =
      Math.pow(dataIPWPairs.map({ case (x, w) => Math.pow(x, w) }).reduce(_ * _), 1 / netIPWeights)
    val expectedGeometricMeanFrequencies =
      Math.pow(dataFrequencyPairs.map({ case (x, w) => Math.pow(x, w) }).reduce(_ * _), 1 / netFrequencies)

    val expectedVariancesFrequencies =
      (1.toDouble / (netFrequencies - 1).toDouble) * dataFrequencyPairs
        .map({ case (x, w) => w * (x - expectedMeanFrequencies) * (x - expectedMeanFrequencies) }).reduce(_ + _)

    val expectedVarianceWeights = (1.toDouble / (netIPWeights - 1).toDouble) *
      dataIPWPairs.map({ case (x, w) => w * (x - expectedMeanIPW) * (x - expectedMeanIPW) }).reduce(_ + _)

    val expectedStandardDeviationFrequencies = Math.sqrt(expectedVariancesFrequencies)
    val expectedStandardDeviationWeights = Math.sqrt(expectedVarianceWeights)

    val expectedSkewnessFrequencies = (dataCount / ((dataCount - 1) * (dataCount - 2))) *
      dataFrequencyPairs.map(
        { case (x, w) => Math.pow(w, 1.5) * Math.pow(((x - expectedMeanFrequencies) / expectedStandardDeviationFrequencies), 3) })
      .reduce(_ + _)

    val expectedSkewnessWeights = (dataCount / ((dataCount - 1) * (dataCount - 2))) *
      dataIPWPairs.map(
        { case (x, w) => Math.pow(w, 1.5) * Math.pow(((x - expectedMeanIPW) / expectedStandardDeviationWeights), 3) })
      .reduce(_ + _)

    val kurtosisMultiplier = dataCount * (dataCount + 1) / ((dataCount - 1) * (dataCount - 2) * (dataCount - 3))
    val kurtosisSubtrahend = 3 * (dataCount - 1) * (dataCount - 1) / ((dataCount - 2) * (dataCount - 3))

    val expectedKurtosisFrequencies = kurtosisMultiplier * dataFrequencyPairs.map(
      { case (x, w) => Math.pow(w, 2) * Math.pow(((x - expectedMeanFrequencies) / expectedStandardDeviationFrequencies), 4) })
      .reduce(_ + _) - kurtosisSubtrahend

    val expectedKurtosisWeights = kurtosisMultiplier * dataIPWPairs.map(
      { case (x, w) => Math.pow(w, 2) * Math.pow(((x - expectedMeanIPW) / expectedStandardDeviationWeights), 4) })
      .reduce(_ + _) - kurtosisSubtrahend
  }

  "mean" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testMean = numericalStatisticsFrequencies.weightedMean

    Math.abs(testMean - expectedMeanFrequencies) should be < epsilon
  }

  "mean" should "handle data with inverse probability weights" in new NumericalStatisticsTestSampleFormulas {

    val testMean = numericalStatisticsWeights.weightedMean

    Math.abs(testMean - expectedMeanIPW) should be < epsilon
  }

  "geometricMean" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testGeometricMean = numericalStatisticsFrequencies.weightedGeometricMean

    Math.abs(testGeometricMean - expectedGeometricMeanFrequencies) should be < epsilon
  }

  "geometricMean" should "handle data with inverse probability weights" in new NumericalStatisticsTestSampleFormulas {
    val testGeometricMean = numericalStatisticsWeights.weightedGeometricMean

    Math.abs(testGeometricMean - expectedGeometricMeanIPW) should be < epsilon
  }

  "variance" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testVariance = numericalStatisticsFrequencies.weightedVariance

    Math.abs(testVariance - expectedVariancesFrequencies) should be < epsilon
  }

  "variance" should "handle data with fractional weights" in new NumericalStatisticsTestSampleFormulas {

    val testVariance = numericalStatisticsWeights.weightedVariance

    Math.abs(testVariance - expectedVarianceWeights) should be < epsilon
  }

  "standard deviation" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testStandardDeviation = numericalStatisticsFrequencies.weightedStandardDeviation

    Math.abs(testStandardDeviation - expectedStandardDeviationFrequencies) should be < epsilon
  }

  "standard deviation" should "handle data with fractional weights" in new NumericalStatisticsTestSampleFormulas {

    val testStandardDeviation = numericalStatisticsWeights.weightedStandardDeviation

    Math.abs(testStandardDeviation - expectedStandardDeviationWeights) should be < epsilon
  }

  "mean confidence lower" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testMCL = numericalStatisticsFrequencies.meanConfidenceLower

    Math.abs(testMCL - (expectedMeanFrequencies - 1.96 * (expectedStandardDeviationFrequencies / Math.sqrt(netFrequencies)))) should be < epsilon
  }

  "mean confidence lower" should "handle data with fractional weights" in new NumericalStatisticsTestSampleFormulas {

    val testMCL = numericalStatisticsWeights.meanConfidenceLower

    Math.abs(testMCL - (expectedMeanIPW - 1.96 * (expectedStandardDeviationWeights / Math.sqrt(netIPWeights)))) should be < epsilon
  }

  "mean confidence upper" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testMCU = numericalStatisticsFrequencies.meanConfidenceUpper

    Math.abs(testMCU - (expectedMeanFrequencies + 1.96 * (expectedStandardDeviationFrequencies / Math.sqrt(netFrequencies)))) should be < epsilon
  }

  "mean confidence upper" should "handle data with fractional weights" in new NumericalStatisticsTestSampleFormulas {

    val testMCU = numericalStatisticsWeights.meanConfidenceUpper

    Math.abs(testMCU - (expectedMeanIPW + 1.96 * (expectedStandardDeviationWeights / Math.sqrt(netIPWeights)))) should be < epsilon
  }

  "max" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testMax = numericalStatisticsFrequencies.max

    testMax shouldBe expectedMax
  }

  "max" should "handle data with fractional weights" in new NumericalStatisticsTestSampleFormulas {

    val testMax = numericalStatisticsWeights.max

    testMax shouldBe expectedMax
  }

  "min" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testMin = numericalStatisticsFrequencies.min

    testMin shouldBe expectedMin
  }

  "min" should "handle data with fractional weights" in new NumericalStatisticsTestSampleFormulas {

    val testMin = numericalStatisticsWeights.min

    testMin shouldBe expectedMin
  }

  "count" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testCount = numericalStatisticsFrequencies.positiveWeightCount

    testCount shouldBe dataCount
  }

  "count" should "handle data with fractional weights" in new NumericalStatisticsTestSampleFormulas {

    val testCount = numericalStatisticsWeights.positiveWeightCount

    testCount shouldBe dataCount
  }

  "total weight" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testTotalWeight = numericalStatisticsFrequencies.totalWeight

    testTotalWeight shouldBe netFrequencies
  }

  "total weight" should "handle data with fractional weights" in new NumericalStatisticsTestSampleFormulas {

    val testTotalWeight = numericalStatisticsWeights.totalWeight

    Math.abs(testTotalWeight - netIPWeights) should be < epsilon
  }

  "skewness" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testSkewness = numericalStatisticsFrequencies.weightedSkewness

    Math.abs(testSkewness - expectedSkewnessFrequencies) should be < epsilon
  }

  "skewness" should "handle data with fractional weights" in new NumericalStatisticsTestSampleFormulas {

    val testSkewness = numericalStatisticsWeights.weightedSkewness

    Math.abs(testSkewness - expectedSkewnessWeights) should be < epsilon
  }

  "kurtosis" should "handle data with integer frequencies" in new NumericalStatisticsTestSampleFormulas {

    val testKurtosis = numericalStatisticsFrequencies.weightedKurtosis

    Math.abs(testKurtosis - expectedKurtosisFrequencies) should be < epsilon
  }

  "kurtosis" should "handle data with fractional weights" in new NumericalStatisticsTestSampleFormulas {

    val testKurtosis = numericalStatisticsWeights.weightedKurtosis

    Math.abs(testKurtosis - expectedKurtosisWeights) should be < epsilon
  }
}
