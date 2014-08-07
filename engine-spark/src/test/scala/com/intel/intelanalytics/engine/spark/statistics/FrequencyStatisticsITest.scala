package com.intel.intelanalytics.engine.spark.statistics

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

    val integers = (1 to 7) :+ 3

    val strings = List("a", "b", "c", "d", "e", "f", "g", "c")

    val integerFrequencies = List(1, 1, 2, 1, 5, 2, 3, 3).map(_.toDouble)

    val expectedModesStrings = Set("c", "e")
    val expectedModesInts = Set(3, 5)

    val expectedModeCount = 2
    val expectedModeWeight = 5.0d

    val netFrequencies = integerFrequencies.reduce(_ + _)
    val fractionalFrequencies: List[Double] = integerFrequencies.map(x => x / netFrequencies)
  }

  "empty data" should "produce mode == None and weights equal to 0" in new FrequencyStatisticsTest {

    val dataList: List[Double] = List()
    val weightList: List[Double] = List()

    val dataWeightPairs = sparkContext.parallelize(dataList.zip(weightList))

    val frequencyStats = new FrequencyStatistics[Double](dataWeightPairs)

    val testMode = frequencyStats.mode
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight
    val testModeCount = frequencyStats.modeCount

    testMode shouldBe None
    testModeWeight shouldBe 0
    testTotalWeight shouldBe 0
    testModeCount shouldBe 0
  }

  "integer data with integer frequencies" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(integers.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs)

    val testMode = frequencyStats.mode.get
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight
    val testModeCount = frequencyStats.modeCount

    expectedModesInts should contain(testMode)
    testModeWeight shouldBe expectedModeWeight
    testTotalWeight shouldBe netFrequencies
    testModeCount shouldBe expectedModeCount

  }

  "string data with integer frequencies" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(strings.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs)

    val testMode = frequencyStats.mode.get
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight
    val testModeCount = frequencyStats.modeCount

    expectedModesStrings should contain(testMode)
    testModeWeight shouldBe expectedModeWeight
    testTotalWeight shouldBe netFrequencies
    testModeCount shouldBe expectedModeCount
  }

  "integer data with fractional weights" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(integers.zip(fractionalFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs)

    val testMode = frequencyStats.mode.get
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight
    val testModeCount = frequencyStats.modeCount

    expectedModesInts should contain(testMode)
    Math.abs(testModeWeight - (expectedModeWeight / netFrequencies.toDouble)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon
    testModeCount shouldBe expectedModeCount

  }

  "string data  with fractional weights" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(strings.zip(fractionalFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs)

    val testMode = frequencyStats.mode.get
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight
    val testModeCount = frequencyStats.modeCount

    expectedModesStrings should contain(testMode)
    Math.abs(testModeWeight - (expectedModeWeight / netFrequencies)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon
    testModeCount shouldBe expectedModeCount
  }

  "items with negative weights" should "not affect mode or total weight" in new FrequencyStatisticsTest {

    val dataWeightPairs: RDD[(String, Double)] =
      sparkContext.parallelize((strings :+ "haha").zip(fractionalFrequencies :+ ((-10.0))))

    val frequencyStats = new FrequencyStatistics[String](dataWeightPairs)

    val testMode = frequencyStats.mode.get
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight
    val testModeCount = frequencyStats.modeCount

    expectedModesStrings should contain(testMode)
    Math.abs(testModeWeight - (expectedModeWeight / netFrequencies)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon
    testModeCount shouldBe expectedModeCount
  }
}
