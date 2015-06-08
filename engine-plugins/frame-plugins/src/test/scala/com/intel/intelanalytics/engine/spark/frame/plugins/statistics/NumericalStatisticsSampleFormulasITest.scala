/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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

    require(data.length == frequencies.length, "Test Data in Error: Data length and frequencies length are mismatched")
    val netFrequencies = frequencies.reduce(_ + _)

    val inverseProbabilityWeights = frequencies.map(x => (netFrequencies.toDouble / x))
    val netIPWeights = inverseProbabilityWeights.reduce(_ + _)

    val dataFrequencyPairs: List[(Double, Double)] = data.zip(frequencies)
    val dataFrequencyPairsAsOptionValues: List[(Option[Double], Option[Double])] = dataFrequencyPairs.map {
      case (k, v) => (Some(k), Some(v))
    }
    val dataFrequencyRDD = sparkContext.parallelize(dataFrequencyPairsAsOptionValues)

    val dataIPWPairs: List[(Double, Double)] = data.zip(inverseProbabilityWeights)
    val dataIPWPairsAsOptionValues: List[(Option[Double], Option[Double])] = dataIPWPairs.map {
      case (k, v) => (Some(k), Some(v))
    }
    val dataIPWRDD = sparkContext.parallelize(dataIPWPairsAsOptionValues)

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

}
