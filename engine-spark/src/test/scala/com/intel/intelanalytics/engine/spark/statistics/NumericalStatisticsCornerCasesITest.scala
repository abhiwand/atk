package com.intel.intelanalytics.engine.spark.statistics

import com.intel.intelanalytics.engine.TestingSparkContext
import org.scalatest.Matchers
import org.scalatest.Assertions
import org.scalacheck.Prop.True

class NumericalStatisticsCornerCasesITest extends TestingSparkContext with Matchers {

  /**
   * Tests some of the corner cases of the numerical statistics routines.
   */
  trait NumericalStatisticsCornerCaseTest {

    val epsilon = 0.000000001
  }

  "numerical statistics" should "ignore zero and negative weighted data" in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 6, 7, 8).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val dataFrequencies = sc.parallelize(data.zip(frequencies))

    val dataFrequenciesWithNegs =
      sc.parallelize((data :+ 1000.toDouble :+ (-10000).toDouble).zip((frequencies :+ (-1).toDouble :+ 0.toDouble)))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)
    val numericalStatisticsWithNegs = new NumericalStatistics(dataFrequenciesWithNegs)

    numericalStatistics.count shouldBe data.length
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

  }

  "numerical statistics" should "give a NaN geometric mean when a data value is negative" in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 6, 7, -18).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val dataFrequencies = sc.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.weightedGeometricMean === Double.NaN

  }

  "numerical statistics" should "give a NaN geometric mean when a data value is 0" in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 0, 7, 18).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val dataFrequencies = sc.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.weightedGeometricMean.isNaN() shouldBe true

  }

  "numerical statistics" should "correctly handle empty data" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List()
    val frequencies: List[Double] = List()

    val dataFrequencies = sc.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.count shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 0) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1) should be < epsilon
    numericalStatistics.min shouldBe Double.PositiveInfinity
    numericalStatistics.max shouldBe Double.NegativeInfinity
    numericalStatistics.weightedVariance.isNaN() shouldBe true
    numericalStatistics.weightedStandardDeviation.isNaN() shouldBe true
    numericalStatistics.weightedSkewness.isNaN() shouldBe true
    numericalStatistics.weightedKurtosis.isNaN() shouldBe true
    numericalStatistics.weightedMode.isNaN() shouldBe true
    numericalStatistics.meanConfidenceLower.isNaN() shouldBe true
    numericalStatistics.meanConfidenceUpper.isNaN() shouldBe true
  }

  "numerical statistics" should "correctly handle data of length 1" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble)
    val frequencies: List[Double] = List(1.toDouble)

    val dataFrequencies = sc.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.count shouldBe 1
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

  "numerical statistics" should "correctly handle data of length 2" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble, 1.toDouble)
    val frequencies: List[Double] = List(1.toDouble, 2.toDouble)

    val dataFrequencies = sc.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.count shouldBe 2
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

  "numerical statistics" should "correctly handle data of length 3, variance 0" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble, 1.toDouble, 1.toDouble)
    val frequencies: List[Double] = List(1.toDouble, 1.toDouble, 1.toDouble)

    val dataFrequencies = sc.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.count shouldBe 3
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

  "numerical statistics" should "correctly handle data of length 3, nonzero variance" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble, 2.toDouble, 1.toDouble)
    val frequencies: List[Double] = List(1.toDouble, 1.toDouble, 1.toDouble)

    val dataFrequencies = sc.parallelize(data.zip(frequencies))

    val numericalStatistics = new NumericalStatistics(dataFrequencies)

    numericalStatistics.count shouldBe 3
    Math.abs(numericalStatistics.weightedMean - 1.333333333) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1.2599210498948732) should be < epsilon
    numericalStatistics.min shouldBe 1
    numericalStatistics.max shouldBe 2
    Math.abs(numericalStatistics.weightedVariance - 0.3333333333) should be < epsilon
    Math.abs(numericalStatistics.weightedStandardDeviation - 0.5773502691896255) should be < epsilon
    (numericalStatistics.weightedSkewness -1.7320508075688807) should be < epsilon
    numericalStatistics.weightedKurtosis.isNaN() shouldBe true
    numericalStatistics.weightedMode shouldBe 1
    Math.abs(numericalStatistics.meanConfidenceLower -  (1.333333333 - (1.96) * (0.5773502691896255 / Math.sqrt(3)))) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceUpper - (1.333333333 + (1.96) * (0.5773502691896255 / Math.sqrt(3)))) should be < epsilon
  }
}
