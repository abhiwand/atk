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
import org.apache.spark.rdd.RDD

/**
 * Tests the frequency statistics package through several corner cases and bad-data cases, as well as "happy path"
 * use cases with both normalized and un-normalized weights.
 */
class FrequencyStatisticsITest extends TestingSparkContextFlatSpec with Matchers {

  trait FrequencyStatisticsTest {

    val epsilon = 0.000000001

    val integers = (1 to 6) :+ 1 :+ 7 :+ 3

    val strings = List("a", "b", "c", "d", "e", "f", "a", "g", "c")

    val integerFrequencies = List(1, 1, 5, 1, 7, 2, 2, 3, 2).map(_.toDouble)

    val modeFrequency = 7.toDouble
    val totalFrequencies = integerFrequencies.reduce(_ + _)

    val fractionalFrequencies: List[Double] = integerFrequencies.map(x => x / totalFrequencies)

    val modeSetInts = Set(3, 5)
    val modeSetStrings = Set("c", "e")

    val firstModeInts = Set(3)
    val firstModeStrings = Set("c")
    val maxReturnCount = 10

  }

  "empty data" should "produce mode == None and weights equal to 0" in new FrequencyStatisticsTest {

    val dataList: List[Double] = List()
    val weightList: List[Double] = List()

    val dataWeightPairs = sparkContext.parallelize(dataList.zip(weightList))

    val frequencyStats = new FrequencyStatistics[Double](dataWeightPairs, maxReturnCount)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet should be('empty)
    testModeWeight shouldBe 0
    testTotalWeight shouldBe 0
  }

  "integer data with integer frequencies" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(integers.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, maxReturnCount)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe modeSetInts
    testModeWeight shouldBe modeFrequency
    testTotalWeight shouldBe totalFrequencies
  }

  "string data with integer frequencies" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(strings.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, maxReturnCount)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe modeSetStrings
    testModeWeight shouldBe modeFrequency
    testTotalWeight shouldBe totalFrequencies
  }

  "integer data with integer frequencies" should "get least mode when asking for just one" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(integers.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, 1)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe firstModeInts
    testModeWeight shouldBe modeFrequency
    testTotalWeight shouldBe totalFrequencies
  }

  "string data with integer frequencies" should "get least mode when asking for just one" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(strings.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, 1)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe firstModeStrings
    testModeWeight shouldBe modeFrequency
    testTotalWeight shouldBe totalFrequencies
  }

  "integer data with fractional weights" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(integers.zip(fractionalFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, maxReturnCount)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe modeSetInts
    Math.abs(testModeWeight - (modeFrequency / totalFrequencies)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon

  }

  "string data  with fractional weights" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(strings.zip(fractionalFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, maxReturnCount)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe modeSetStrings
    Math.abs(testModeWeight - (modeFrequency / totalFrequencies)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon
  }

  "items with negative weights" should "not affect mode or total weight" in new FrequencyStatisticsTest {

    val dataWeightPairs: RDD[(String, Double)] =
      sparkContext.parallelize((strings :+ "haha").zip(fractionalFrequencies :+ ((-10.0))))

    val frequencyStats = new FrequencyStatistics[String](dataWeightPairs, maxReturnCount)

    val testMode = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testMode shouldBe modeSetStrings
    Math.abs(testModeWeight - (modeFrequency / totalFrequencies)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon
  }
}
