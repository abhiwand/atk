package com.intel.intelanalytics.engine.spark.statistics

import org.scalatest.Matchers
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.intelanalytics.engine.spark.statistics.numericalstatistics.NumericalStatistics

class NumericalStatisticsPopulationFormulasITest extends TestingSparkContextFlatSpec with Matchers {

  /**
   * Tests the distributed implementation of the statistics calculator against the formulae for population parameters.
   *
   * It tests whether or not the distributed implementations (with the accumulators and
   * whatnot) match the textbook formulae on a small data set.
   */
  trait NumericalStatisticsTestPopulationFormulas {

    val epsilon = 0.000000001

    val data = List(1, 2, 3, 4, 5, 6, 7, 8, 9).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1, 9).map(x => x.toDouble)

    require(data.length > 3, "Test Data in Error: Data should have at least four elements, lest the kurtosis be trivialized.")
    require(data.length == frequencies.length, "Test Data in Error: Data length and frequencies length are mismatched")
    val netFrequencies = frequencies.reduce(_ + _)

    val normalizedWeights = frequencies.map(x => x / (netFrequencies.toDouble))
    val netWeight = normalizedWeights.reduce(_ + _)

    val dataFrequencyPairs: List[(Double, Double)] = data.zip(frequencies)
    val dataFrequencyRDD = sparkContext.parallelize(dataFrequencyPairs)

    val dataWeightPairs: List[(Double, Double)] = data.zip(normalizedWeights)
    val dataWeightRDD = sparkContext.parallelize(dataWeightPairs)

    val numericalStatisticsFrequencies = new NumericalStatistics(dataFrequencyRDD, true)

    val numericalStatisticsWeights = new NumericalStatistics(dataWeightRDD, true)

    val expectedMean: Double = dataWeightPairs.map({ case (x, w) => x * w }).reduce(_ + _)
    val expectedMax: Double = data.reduce(Math.max(_, _))
    val expectedMin: Double = data.reduce(Math.min(_, _))
    val dataCount: Double = data.length

    val expectedGeometricMean = dataWeightPairs.map({ case (x, w) => Math.pow(x, w) }).reduce(_ * _)

    val expectedVariancesFrequencies = (1.toDouble / (netFrequencies).toDouble) *
      dataFrequencyPairs.map({ case (x, w) => w * (x - expectedMean) * (x - expectedMean) }).reduce(_ + _)

    val expectedVarianceWeights = (1.toDouble / (netWeight).toDouble) *
      dataWeightPairs.map({ case (x, w) => w * (x - expectedMean) * (x - expectedMean) }).reduce(_ + _)

    val expectedStandardDeviationFrequencies = Math.sqrt(expectedVariancesFrequencies)
    val expectedStandardDeviationWeights = Math.sqrt(expectedVarianceWeights)

    val expectedSkewnessFrequencies = (dataCount / ((dataCount - 1) * (dataCount - 2))) *
      dataFrequencyPairs.map(
        { case (x, w) => Math.pow(w, 1.5) * Math.pow(((x - expectedMean) / expectedStandardDeviationFrequencies), 3) })
      .reduce(_ + _)

    val expectedSkewnessWeights = (dataCount / ((dataCount - 1) * (dataCount - 2))) *
      dataWeightPairs.map(
        { case (x, w) => Math.pow(w, 1.5) * Math.pow(((x - expectedMean) / expectedStandardDeviationWeights), 3) })
      .reduce(_ + _)

    // TODO - the weighted skewness and kurtosis calculations are probably off! should be cleaned before open sourcing
    // lest we be very embarassed...
    val kurtosisMultiplier = dataCount * (dataCount + 1) / ((dataCount - 1) * (dataCount - 2) * (dataCount - 3))
    val kurtosisSubtrahend = 3 * (dataCount - 1) * (dataCount - 1) / ((dataCount - 2) * (dataCount - 3))

    val expectedKurtosisFrequencies = kurtosisMultiplier * dataFrequencyPairs.map(
      { case (x, w) => Math.pow(w, 2) * Math.pow(((x - expectedMean) / expectedStandardDeviationFrequencies), 4) })
      .reduce(_ + _) - kurtosisSubtrahend

    val expectedKurtosisWeights = kurtosisMultiplier * dataWeightPairs.map(
      { case (x, w) => Math.pow(w, 2) * Math.pow(((x - expectedMean) / expectedStandardDeviationWeights), 4) })
      .reduce(_ + _) - kurtosisSubtrahend
  }

  "mean" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testMean = numericalStatisticsFrequencies.weightedMean

    Math.abs(testMean - expectedMean) should be < epsilon
  }

  "mean" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testMean = numericalStatisticsWeights.weightedMean

    Math.abs(testMean - expectedMean) should be < epsilon
  }

  "geometricMean" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testGeometricMean = numericalStatisticsFrequencies.weightedGeometricMean

    Math.abs(testGeometricMean - expectedGeometricMean) should be < epsilon
  }

  "geometricMean" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {
    val testGeometricMean = numericalStatisticsWeights.weightedGeometricMean

    Math.abs(testGeometricMean - expectedGeometricMean) should be < epsilon
  }

  "variance" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testVariance = numericalStatisticsFrequencies.weightedVariance

    Math.abs(testVariance - expectedVariancesFrequencies) should be < epsilon
  }

  "variance" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testVariance = numericalStatisticsWeights.weightedVariance

    Math.abs(testVariance - expectedVarianceWeights) should be < epsilon
  }

  "standard deviation" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testStandardDeviation = numericalStatisticsFrequencies.weightedStandardDeviation

    Math.abs(testStandardDeviation - expectedStandardDeviationFrequencies) should be < epsilon
  }

  "standard deviation" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testStandardDeviation = numericalStatisticsWeights.weightedStandardDeviation

    Math.abs(testStandardDeviation - expectedStandardDeviationWeights) should be < epsilon
  }

  "max" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testMax = numericalStatisticsFrequencies.max

    testMax shouldBe expectedMax
  }

  "max" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testMax = numericalStatisticsWeights.max

    testMax shouldBe expectedMax
  }

  "min" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testMin = numericalStatisticsFrequencies.min

    testMin shouldBe expectedMin
  }

  "min" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testMin = numericalStatisticsWeights.min

    testMin shouldBe expectedMin
  }

  "count" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testCount = numericalStatisticsFrequencies.positiveWeightCount

    testCount shouldBe dataCount
  }

  "count" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testCount = numericalStatisticsWeights.positiveWeightCount

    testCount shouldBe dataCount
  }

  "total weight" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testTotalWeight = numericalStatisticsFrequencies.totalWeight

    testTotalWeight shouldBe netFrequencies
  }

  "total weight" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testTotalWeight = numericalStatisticsWeights.totalWeight

    Math.abs(testTotalWeight - netWeight) should be < epsilon
  }

  "skewness" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testSkewness = numericalStatisticsFrequencies.weightedSkewness

    Math.abs(testSkewness - expectedSkewnessFrequencies) should be < epsilon
  }

  "skewness" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testSkewness = numericalStatisticsWeights.weightedSkewness

    Math.abs(testSkewness - expectedSkewnessWeights) should be < epsilon
  }

  "kurtosis" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testKurtosis = numericalStatisticsFrequencies.weightedKurtosis

    Math.abs(testKurtosis - expectedKurtosisFrequencies) should be < epsilon
  }

  "kurtosis" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testKurtosis = numericalStatisticsWeights.weightedKurtosis

    Math.abs(testKurtosis - expectedKurtosisWeights) should be < epsilon
  }
}
