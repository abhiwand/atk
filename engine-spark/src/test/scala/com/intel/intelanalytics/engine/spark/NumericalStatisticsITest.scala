package com.intel.intelanalytics.engine.spark

import org.scalatest.{ Matchers, FunSuite }
import com.intel.intelanalytics.engine.TestingSparkContext

class NumericalStatisticsITest extends TestingSparkContext with Matchers {

  trait NumericalStatisticsTest {

    val epsilon = 0.000000001

    val data = List(1, 2, 3, 4).map(x => x.toDouble)

    val frequencies = List(3, 2, 3, 1).map(x => x.toDouble)

    val weights = frequencies.map(x => x / (9.toDouble))

    val dataFrequencyPairs = sc.parallelize(data.zip(frequencies))
    val dataWeightPairs = sc.parallelize(data.zip(weights))

    val expectedMean = 2.222222222222
    val expectedMax = 4.toDouble
    val expectedMin = 1.toDouble
    val expectedCount = 4
    val expectedGeometricMean = 1.962598793

    val expectedModes = Set(1.toDouble, 3.toDouble)

    // variance and standard deviation are calculated by the formulas from:
    // http://support.sas.com/documentation/cdl/en/procstat/63104/HTML/default/viewer.htm#procstat_univariate_sect026.htm
    //  !!! with the default setting, d = 1/(n-1), so that the results differ for frequency and normalized weights)

    val expectedVariancesFrequencies = (1.toDouble / (data.length - 1).toDouble) *
      data.zip(frequencies).map({ case (x, w) => w * (x - expectedMean) * (x - expectedMean) }).reduce(_ + _)

    val expectedVarianceWeights = (1.toDouble / (data.length - 1).toDouble) *
      data.zip(weights).map({ case (x, w) => w * (x - expectedMean) * (x - expectedMean) }).reduce(_ + _)

    val expectedStandardDeviationFrequencies = Math.sqrt(expectedVariancesFrequencies)
    val expectedStandardDeviationWeights = Math.sqrt(expectedVarianceWeights)
  }

  "mean" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyPairs)

    val testMean = numericalStatistics.summaryStatistics.mean

    Math.abs(testMean - expectedMean) should be < epsilon
  }

  "mean" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightPairs)

    val testMean = numericalStatistics.summaryStatistics.mean

    Math.abs(testMean - expectedMean) should be < epsilon
  }

  "geometricMean" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyPairs)

    val testGeometricMean = numericalStatistics.summaryStatistics.geometric_mean

    Math.abs(testGeometricMean - expectedGeometricMean) should be < epsilon
  }

  "geometricMean" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightPairs)

    val testGeometricMean = numericalStatistics.summaryStatistics.geometric_mean

    Math.abs(testGeometricMean - expectedGeometricMean) should be < epsilon
  }

  "variance" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyPairs)

    val testVariance = numericalStatistics.summaryStatistics.variance

    Math.abs(testVariance - expectedVariancesFrequencies) should be < epsilon
  }

  "variance" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightPairs)

    val testVariance = numericalStatistics.summaryStatistics.variance

    Math.abs(testVariance - expectedVarianceWeights) should be < epsilon
  }

  "standard deviation" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyPairs)

    val testStandardDeviation = numericalStatistics.summaryStatistics.standard_deviation

    Math.abs(testStandardDeviation - expectedStandardDeviationFrequencies) should be < epsilon
  }

  "standard deviation" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightPairs)

    val testStandardDeviation = numericalStatistics.summaryStatistics.standard_deviation

    Math.abs(testStandardDeviation - expectedStandardDeviationWeights) should be < epsilon
  }

  "max" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyPairs)

    val testMax = numericalStatistics.summaryStatistics.maximum

    testMax shouldBe expectedMax
  }

  "max" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightPairs)

    val testMax = numericalStatistics.summaryStatistics.maximum

    testMax shouldBe expectedMax
  }

  "min" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyPairs)

    val testMin = numericalStatistics.summaryStatistics.minimum

    testMin shouldBe expectedMin
  }

  "min" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightPairs)

    val testMin = numericalStatistics.summaryStatistics.minimum

    testMin shouldBe expectedMin
  }

  "count" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyPairs)

    val testCount = numericalStatistics.summaryStatistics.count

    testCount shouldBe expectedCount
  }

  "count" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightPairs)

    val testCount = numericalStatistics.summaryStatistics.count

    testCount shouldBe expectedCount
  }

  "mode" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyPairs)

    val testMode = numericalStatistics.summaryStatistics.mode

    expectedModes should contain(testMode)
  }

  "mode" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightPairs)

    val testMode = numericalStatistics.summaryStatistics.mode

    expectedModes should contain(testMode)
  }
}
