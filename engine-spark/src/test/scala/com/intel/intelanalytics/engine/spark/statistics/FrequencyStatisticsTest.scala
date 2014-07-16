package com.intel.intelanalytics.engine.spark.statistics

import org.scalatest.Matchers
import com.intel.intelanalytics.engine.TestingSparkContext

class FrequencyStatisticsITest extends TestingSparkContext with Matchers {

  trait FrequencyStatisticsTest {

    val epsilon = 0.000000001

    val integers = 1 to 7

    val strings = List("a", "b", "c", "d", "e", "f", "g")

    val integerFrequencies = List(1, 1, 5, 1, 1, 2, 3).map(_.toDouble)

    val fractionalFrequencies = integerFrequencies.map(x => x / 14.0)

  }

  "modeAndWeight" should "handle integer data with integer frequencies" in new FrequencyStatisticsTest {

    val dataWeightPairs = sc.parallelize(integers.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, 0)

    val (testMode, testModeWeight, testTotalWeight) = frequencyStats.modeItsWeightTotalWeightTriple

    testMode shouldBe 3
    testModeWeight shouldBe 5
    testTotalWeight shouldBe 14
  }

  "modeAndWeight" should "handle strings with integer frequencies" in new FrequencyStatisticsTest {

    val dataWeightPairs = sc.parallelize(strings.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, "<<no strings seen>>")

    val (testMode, testModeWeight, testTotalWeight) = frequencyStats.modeItsWeightTotalWeightTriple

    testMode shouldBe "c"
    testModeWeight shouldBe 5
    testTotalWeight shouldBe 14
  }

  "modeAndWeight" should "handle integer data with fractional weights" in new FrequencyStatisticsTest {

    val dataWeightPairs = sc.parallelize(integers.zip(fractionalFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, 0)

    val (testMode, testModeWeight, testTotalWeight) = frequencyStats.modeItsWeightTotalWeightTriple

    testMode shouldBe 3
    Math.abs(testModeWeight - (5.toDouble / 14.toDouble)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon

  }

  "modeAndWeight" should "handle weighted strings" in new FrequencyStatisticsTest {

    val dataWeightPairs = sc.parallelize(strings.zip(fractionalFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, "<<no strings seen>>")

    val (testMode, testModeWeight, testTotalWeight) = frequencyStats.modeItsWeightTotalWeightTriple

    testMode shouldBe "c"
    Math.abs(testModeWeight - (5.toDouble / 14.toDouble)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon
  }
}
