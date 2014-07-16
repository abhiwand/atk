package com.intel.intelanalytics.engine.spark

import org.scalatest.{ Matchers, FunSuite }
import com.intel.intelanalytics.engine.TestingSparkContext

/**
 * Tests the distributed implementation of the statistics calculator against the standard definitions.
 */
class NumericalStatisticsITest extends TestingSparkContext with Matchers {

  trait NumericalStatisticsTest {

    val epsilon = 0.000000001

    // the only restriction on the test data is that the mode should be unique, otherwise it's very hard to test
    val data = List(1, 2, 3, 4, 5, 6, 7, 8).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    require(data.length == frequencies.length, "Test Data in Error: Data length and frequencies length are mismatched")
    val totalWeight = frequencies.reduce(_ + _)

    val normalizedWeights = frequencies.map(x => x / (totalWeight.toDouble))

    val dataFrequencyPairs: List[(Double, Double)] = data.zip(frequencies)
    val dataFrequencyRDD = sc.parallelize(dataFrequencyPairs)

    val dataWeightPairs: List[(Double, Double)] = data.zip(normalizedWeights)
    val dataWeightRDD = sc.parallelize(dataWeightPairs)

    // Formulas for statistics are expected to adhere to the DEFAULT formulas used by SAS in
    // http://support.sas.com/documentation/cdl/en/procstat/63104/HTML/default/viewer.htm#procstat_univariate_sect026.ht

    val expectedMean: Double = dataWeightPairs.map({ case (x, w) => x * w }).reduce(_ + _)
    val expectedMax: Double = data.reduce(Math.max(_, _))
    val expectedMin: Double = data.reduce(Math.min(_, _))
    val expectedCount: Double = data.length

    val expectedGeometricMean = dataWeightPairs.map({ case (x, w) => Math.pow(x, w) }).reduce(_ * _)

    val expectedMode = dataWeightPairs.reduce(findMode)._1
    private def findMode(x: (Double, Double), y: (Double, Double)) = if (x._2 > y._2) x else y

    // variance and standard deviation are calculated by the formulas from:

    //  !!! with the default setting, d = 1/(n-1), so that the results differ for frequency and normalized weights)

    val expectedVariancesFrequencies = (1.toDouble / (data.length - 1).toDouble) *
      dataFrequencyPairs.map({ case (x, w) => w * (x - expectedMean) * (x - expectedMean) }).reduce(_ + _)

    val expectedVarianceWeights = (1.toDouble / (data.length - 1).toDouble) *
      dataWeightPairs.map({ case (x, w) => w * (x - expectedMean) * (x - expectedMean) }).reduce(_ + _)

    val expectedStandardDeviationFrequencies = Math.sqrt(expectedVariancesFrequencies)
    val expectedStandardDeviationWeights = Math.sqrt(expectedVarianceWeights)
  }

  "mean" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyRDD)

    val testMean = numericalStatistics.summaryStatistics.mean

    Math.abs(testMean - expectedMean) should be < epsilon
  }

  "mean" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightRDD)

    val testMean = numericalStatistics.summaryStatistics.mean

    Math.abs(testMean - expectedMean) should be < epsilon
  }

  "geometricMean" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyRDD)

    val testGeometricMean = numericalStatistics.summaryStatistics.geometric_mean

    Math.abs(testGeometricMean - expectedGeometricMean) should be < epsilon
  }

  "geometricMean" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightRDD)

    val testGeometricMean = numericalStatistics.summaryStatistics.geometric_mean

    Math.abs(testGeometricMean - expectedGeometricMean) should be < epsilon
  }

  "variance" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyRDD)

    val testVariance = numericalStatistics.summaryStatistics.variance

    Math.abs(testVariance - expectedVariancesFrequencies) should be < epsilon
  }

  "variance" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightRDD)

    val testVariance = numericalStatistics.summaryStatistics.variance

    Math.abs(testVariance - expectedVarianceWeights) should be < epsilon
  }

  "standard deviation" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyRDD)

    val testStandardDeviation = numericalStatistics.summaryStatistics.standard_deviation

    Math.abs(testStandardDeviation - expectedStandardDeviationFrequencies) should be < epsilon
  }

  "standard deviation" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightRDD)

    val testStandardDeviation = numericalStatistics.summaryStatistics.standard_deviation

    Math.abs(testStandardDeviation - expectedStandardDeviationWeights) should be < epsilon
  }

  "max" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyRDD)

    val testMax = numericalStatistics.summaryStatistics.maximum

    testMax shouldBe expectedMax
  }

  "max" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightRDD)

    val testMax = numericalStatistics.summaryStatistics.maximum

    testMax shouldBe expectedMax
  }

  "min" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyRDD)

    val testMin = numericalStatistics.summaryStatistics.minimum

    testMin shouldBe expectedMin
  }

  "min" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightRDD)

    val testMin = numericalStatistics.summaryStatistics.minimum

    testMin shouldBe expectedMin
  }

  "count" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyRDD)

    val testCount = numericalStatistics.summaryStatistics.count

    testCount shouldBe expectedCount
  }

  "count" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightRDD)

    val testCount = numericalStatistics.summaryStatistics.count

    testCount shouldBe expectedCount
  }

  "mode" should "handle data with integer frequencies" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataFrequencyRDD)

    val testMode = numericalStatistics.summaryStatistics.mode

    testMode shouldBe expectedMode
  }

  "mode" should "handle data with fractional weights" in new NumericalStatisticsTest {

    val numericalStatistics = new NumericalStatistics(dataWeightRDD)

    val testMode = numericalStatistics.summaryStatistics.mode

    testMode shouldBe expectedMode
  }
}
