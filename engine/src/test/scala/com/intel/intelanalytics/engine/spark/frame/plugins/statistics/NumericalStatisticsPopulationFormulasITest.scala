//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics

import org.scalatest.Matchers
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.numericalstatistics.NumericalStatistics

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

    require(data.length == frequencies.length, "Test Data in Error: Data length and frequencies length are mismatched")
    val netFrequencies = frequencies.reduce(_ + _)

    val normalizedWeights = frequencies.map(x => x / netFrequencies.toDouble)
    val netWeight = normalizedWeights.reduce(_ + _)

    val dataFrequencyPairs: List[(Double, Double)] = data.zip(frequencies)
    val dataFrequencyPairsAsOptionValues: List[(Option[Double], Option[Double])] = dataFrequencyPairs.map {
      case (k, v) => (Some(k), Some(v))
    }
    val dataFrequencyRDD = sparkContext.parallelize(dataFrequencyPairsAsOptionValues)

    val dataWeightPairs: List[(Double, Double)] = data.zip(normalizedWeights)
    val dataWeightPairsAsOptionValues: List[(Option[Double], Option[Double])] = dataWeightPairs.map {
      case (k, v) => (Some(k), Some(v))
    }
    val dataWeightRDD = sparkContext.parallelize(dataWeightPairsAsOptionValues)

    val numericalStatisticsFrequencies = new NumericalStatistics(dataFrequencyRDD, true)

    val numericalStatisticsWeights = new NumericalStatistics(dataWeightRDD, true)

    val expectedMean: Double = dataWeightPairs.map({ case (x, w) => x * w }).reduce(_ + _)
    val expectedMax: Double = data.reduce(Math.max(_, _))
    val expectedMin: Double = data.reduce(Math.min(_, _))
    val dataCount: Double = data.length

    val expectedGeometricMean = dataWeightPairs.map({ case (x, w) => Math.pow(x, w) }).reduce(_ * _)

    val expectedVariancesFrequencies = (1.toDouble / netFrequencies) *
      dataFrequencyPairs.map({ case (x, w) => w * (x - expectedMean) * (x - expectedMean) }).reduce(_ + _)

    val expectedVarianceWeights = (1.toDouble / netWeight) *
      dataWeightPairs.map({ case (x, w) => w * (x - expectedMean) * (x - expectedMean) }).reduce(_ + _)

    val expectedStandardDeviationFrequencies = Math.sqrt(expectedVariancesFrequencies)
    val expectedStandardDeviationWeights = Math.sqrt(expectedVarianceWeights)

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

  "mean confidence lower" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testMCL = numericalStatisticsFrequencies.meanConfidenceLower

    Math.abs(testMCL - (expectedMean - 1.96 * (expectedStandardDeviationFrequencies / Math.sqrt(netFrequencies)))) should be < epsilon
  }

  "mean confidence lower" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testMCL = numericalStatisticsWeights.meanConfidenceLower

    Math.abs(testMCL - (expectedMean - 1.96 * (expectedStandardDeviationWeights / Math.sqrt(netWeight)))) should be < epsilon
  }

  "mean confidence upper" should "handle data with integer frequencies" in new NumericalStatisticsTestPopulationFormulas {

    val testMCU = numericalStatisticsFrequencies.meanConfidenceUpper

    Math.abs(testMCU - (expectedMean + 1.96 * (expectedStandardDeviationFrequencies / Math.sqrt(netFrequencies)))) should be < epsilon
  }

  "mean confidence upper" should "handle data with fractional weights" in new NumericalStatisticsTestPopulationFormulas {

    val testMCU = numericalStatisticsWeights.meanConfidenceUpper

    Math.abs(testMCU - (expectedMean + 1.96 * (expectedStandardDeviationWeights / Math.sqrt(netWeight)))) should be < epsilon
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

}
