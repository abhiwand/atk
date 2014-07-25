package com.intel.intelanalytics.engine.spark.statistics

import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers
import org.scalatest.Assertions
import org.scalacheck.Prop.True
import com.intel.intelanalytics.engine.spark.statistics.numericalstatistics.NumericalStatistics

class NumericalStatisticsCornerCasesITest extends TestingSparkContextFlatSpec with Matchers {

  /**
   * Tests some of the corner cases of the numerical statistics routines: NaNs, infinite values, divide by 0,
   * logarithms of negative numbers, etc. Floating point exceptions, if you catch my drift.
   */
  trait NumericalStatisticsCornerCaseTest {

    val epsilon = 0.000000001
  }

  "values with non-positive weights" should "be ignored , except for calculating count of non-positive weight data " in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 6, 7, 8).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val dataFrequenciesWithNegs =
      sparkContext.parallelize((data :+ 1000.toDouble :+ (-10000).toDouble).zip((frequencies :+ (-1).toDouble :+ 0.toDouble)))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)
    val numericalStatisticsWithNegs = new NumericalStatistics(dataFrequenciesWithNegs)

    numericalStatistics.positiveWeightCount shouldBe data.length
    numericalStatisticsWithNegs.positiveWeightCount shouldBe data.length

    Math.abs(numericalStatistics.weightedMean - numericalStatisticsWithNegs.weightedMean) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean
      - numericalStatisticsWithNegs.weightedGeometricMean) should be < epsilon
    Math.abs(numericalStatistics.min - numericalStatisticsWithNegs.min) should be < epsilon
    Math.abs(numericalStatistics.max - numericalStatisticsWithNegs.max) should be < epsilon
    Math.abs(numericalStatistics.weightedVariance - numericalStatisticsWithNegs.weightedVariance) should be < epsilon
    Math.abs(numericalStatistics.weightedStandardDeviation
      - numericalStatisticsWithNegs.weightedStandardDeviation) should be < epsilon
    Math.abs(numericalStatistics.weightedSkewness - numericalStatisticsWithNegs.weightedSkewness) should be < epsilon
    Math.abs(numericalStatistics.weightedKurtosis - numericalStatisticsWithNegs.weightedKurtosis) should be < epsilon
    Math.abs(numericalStatistics.weightedMode - numericalStatisticsWithNegs.weightedMode) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceLower
      - numericalStatisticsWithNegs.meanConfidenceLower) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceUpper
      - numericalStatisticsWithNegs.meanConfidenceUpper) should be < epsilon

    numericalStatistics.nonPositiveWeightCount shouldBe 0
    numericalStatisticsWithNegs.nonPositiveWeightCount shouldBe 2

  }

  "values with NaN or infinite weights" should "be ignored , except for calculating count of bad rows " in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 6, 7, 8).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val goodDataBadWeights = (1 to 3).map(x => x.toDouble).zip(List(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity))
    val badDataGoodWeights = List(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).zip((1 to 3).map(x => x.toDouble))

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val dataFrequenciesWithBadGuys =
      sparkContext.parallelize(data.zip(frequencies) ++ badDataGoodWeights ++ goodDataBadWeights)

    val numericalStatistics = new NumericalStatistics(dataFrequencies)
    val numericalStatisticsWithBadGuys = new NumericalStatistics(dataFrequenciesWithBadGuys)

    numericalStatistics.positiveWeightCount shouldBe data.length
    numericalStatisticsWithBadGuys.positiveWeightCount shouldBe data.length

    numericalStatistics.badRowCount shouldBe 0
    numericalStatistics.goodRowCount shouldBe data.length

    numericalStatisticsWithBadGuys.badRowCount shouldBe 6
    numericalStatisticsWithBadGuys.goodRowCount shouldBe data.length

    Math.abs(numericalStatistics.weightedMean - numericalStatisticsWithBadGuys.weightedMean) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean
      - numericalStatisticsWithBadGuys.weightedGeometricMean) should be < epsilon
    Math.abs(numericalStatistics.min - numericalStatisticsWithBadGuys.min) should be < epsilon
    Math.abs(numericalStatistics.max - numericalStatisticsWithBadGuys.max) should be < epsilon
    Math.abs(numericalStatistics.weightedVariance - numericalStatisticsWithBadGuys.weightedVariance) should be < epsilon
    Math.abs(numericalStatistics.weightedStandardDeviation
      - numericalStatisticsWithBadGuys.weightedStandardDeviation) should be < epsilon
    Math.abs(numericalStatistics.weightedSkewness - numericalStatisticsWithBadGuys.weightedSkewness) should be < epsilon
    Math.abs(numericalStatistics.weightedKurtosis - numericalStatisticsWithBadGuys.weightedKurtosis) should be < epsilon
    Math.abs(numericalStatistics.weightedMode - numericalStatisticsWithBadGuys.weightedMode) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceLower
      - numericalStatisticsWithBadGuys.meanConfidenceLower) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceUpper
      - numericalStatisticsWithBadGuys.meanConfidenceUpper) should be < epsilon

    numericalStatistics.nonPositiveWeightCount shouldBe 0
    numericalStatisticsWithBadGuys.nonPositiveWeightCount shouldBe 0

  }
  "when a data value is negative" should "give a NaN geometric mean" in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 6, 7, -18).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.weightedGeometricMean.isNaN() shouldBe true

  }

  "a data value is 0" should "give a NaN geometric mean " in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 0, 7, 18).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.weightedGeometricMean.isNaN() shouldBe true

  }

  "empty data" should "provide expected statistics" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List()
    val frequencies: List[Double] = List()

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.positiveWeightCount shouldBe 0
    numericalStatistics.nonPositiveWeightCount shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 0) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1) should be < epsilon
    numericalStatistics.min.isNaN() shouldBe true
    numericalStatistics.max.isNaN() shouldBe true
    numericalStatistics.weightedVariance.isNaN() shouldBe true
    numericalStatistics.weightedStandardDeviation.isNaN() shouldBe true
    numericalStatistics.weightedSkewness.isNaN() shouldBe true
    numericalStatistics.weightedKurtosis.isNaN() shouldBe true
    numericalStatistics.weightedMode.isNaN() shouldBe true
    numericalStatistics.meanConfidenceLower.isNaN() shouldBe true
    numericalStatistics.meanConfidenceUpper.isNaN() shouldBe true
  }

  "all data has negative weights" should "be like empty data but with correct non-positive count" in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 6, 7, -18).map(x => x.toDouble)
    val frequencies = List(-3, -2, -3, -1, -9, -4, -3, -1).map(x => x.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.positiveWeightCount shouldBe 0
    numericalStatistics.nonPositiveWeightCount shouldBe data.length
    Math.abs(numericalStatistics.weightedMean - 0) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1) should be < epsilon
    numericalStatistics.min.isNaN() shouldBe true
    numericalStatistics.max.isNaN() shouldBe true
    numericalStatistics.weightedVariance.isNaN() shouldBe true
    numericalStatistics.weightedStandardDeviation.isNaN() shouldBe true
    numericalStatistics.weightedSkewness.isNaN() shouldBe true
    numericalStatistics.weightedKurtosis.isNaN() shouldBe true
    numericalStatistics.weightedMode.isNaN() shouldBe true
    numericalStatistics.meanConfidenceLower.isNaN() shouldBe true
    numericalStatistics.meanConfidenceUpper.isNaN() shouldBe true

  }

  "data of length 1" should "work" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble)
    val frequencies: List[Double] = List(1.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.positiveWeightCount shouldBe 1
    numericalStatistics.nonPositiveWeightCount shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 1) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1) should be < epsilon
    numericalStatistics.min shouldBe 1
    numericalStatistics.max shouldBe 1
    numericalStatistics.weightedVariance.isNaN() shouldBe true
    numericalStatistics.weightedStandardDeviation.isNaN() shouldBe true
    numericalStatistics.weightedSkewness.isNaN() shouldBe true
    numericalStatistics.weightedKurtosis.isNaN() shouldBe true
    numericalStatistics.weightedMode shouldBe 1
    numericalStatistics.meanConfidenceLower.isNaN() shouldBe true
    numericalStatistics.meanConfidenceUpper.isNaN() shouldBe true
  }

  "data of length 2" should "work" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble, 1.toDouble)
    val frequencies: List[Double] = List(1.toDouble, 2.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.positiveWeightCount shouldBe 2
    numericalStatistics.nonPositiveWeightCount shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 1) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1) should be < epsilon
    numericalStatistics.min shouldBe 1
    numericalStatistics.max shouldBe 1
    numericalStatistics.weightedVariance shouldBe 0
    numericalStatistics.weightedStandardDeviation shouldBe 0
    numericalStatistics.weightedSkewness.isNaN() shouldBe true
    numericalStatistics.weightedKurtosis.isNaN() shouldBe true
    numericalStatistics.weightedMode shouldBe 1
    numericalStatistics.meanConfidenceLower shouldBe 1
    numericalStatistics.meanConfidenceUpper shouldBe 1
  }

  "data of length 3, variance 0" should "work" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble, 1.toDouble, 1.toDouble)
    val frequencies: List[Double] = List(1.toDouble, 1.toDouble, 1.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.positiveWeightCount shouldBe 3
    numericalStatistics.nonPositiveWeightCount shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 1) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1) should be < epsilon
    numericalStatistics.min shouldBe 1
    numericalStatistics.max shouldBe 1
    numericalStatistics.weightedVariance shouldBe 0
    numericalStatistics.weightedStandardDeviation shouldBe 0
    numericalStatistics.weightedSkewness.isNaN() shouldBe true
    numericalStatistics.weightedKurtosis.isNaN() shouldBe true
    numericalStatistics.weightedMode shouldBe 1
    numericalStatistics.meanConfidenceLower shouldBe 1
    numericalStatistics.meanConfidenceUpper shouldBe 1
  }

  "data of length 3, nonzero variance" should "work" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble, 2.toDouble, 1.toDouble)
    val frequencies: List[Double] = List(1.toDouble, 1.toDouble, 1.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.positiveWeightCount shouldBe 3
    numericalStatistics.nonPositiveWeightCount shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 1.333333333) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1.2599210498948732) should be < epsilon
    numericalStatistics.min shouldBe 1
    numericalStatistics.max shouldBe 2
    Math.abs(numericalStatistics.weightedVariance - 0.3333333333) should be < epsilon
    Math.abs(numericalStatistics.weightedStandardDeviation - 0.5773502691896255) should be < epsilon
    (numericalStatistics.weightedSkewness - 1.7320508075688807) should be < epsilon
    numericalStatistics.weightedKurtosis.isNaN() shouldBe true
    numericalStatistics.weightedMode shouldBe 1
    Math.abs(numericalStatistics.meanConfidenceLower - (1.333333333 - (1.96) * (0.5773502691896255 / Math.sqrt(3)))) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceUpper - (1.333333333 + (1.96) * (0.5773502691896255 / Math.sqrt(3)))) should be < epsilon
  }

  "uniform data" should "work" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(2, 2, 2, 2, 2).map(x => x.toDouble)
    val frequencies: List[Double] = List(3, 3, 3, 3, 3).map(x => x.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.positiveWeightCount shouldBe 5
    numericalStatistics.nonPositiveWeightCount shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 2) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 2) should be < epsilon
    numericalStatistics.min shouldBe 2
    numericalStatistics.max shouldBe 2
    numericalStatistics.weightedVariance shouldBe 0
    numericalStatistics.weightedStandardDeviation shouldBe 0
    numericalStatistics.weightedSkewness.isNaN() shouldBe true
    numericalStatistics.weightedKurtosis.isNaN() shouldBe true
    numericalStatistics.weightedMode shouldBe 2
    numericalStatistics.meanConfidenceLower shouldBe 2
    numericalStatistics.meanConfidenceUpper shouldBe 2
  }

  "competing modes" should "result in the least mode" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(3, 1, 4, 5, 2).map(x => x.toDouble)
    val frequencies: List[Double] = List(1, 1, 2, 1, 2).map(x => x.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.weightedMode shouldBe 2
  }
}
