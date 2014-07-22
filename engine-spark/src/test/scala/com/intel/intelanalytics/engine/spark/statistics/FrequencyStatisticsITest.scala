package com.intel.intelanalytics.engine.spark.statistics

import org.scalatest.Matchers
import com.intel.intelanalytics.engine.TestingSparkContext
import org.apache.spark.rdd.RDD

/**
 * Tests the frequency statistics package through several corner cases and bad-data cases, as well as "happy path"
 * use cases with both normalized and un-normalized weights.
 */
class FrequencyStatisticsITest extends TestingSparkContext with Matchers {

  trait FrequencyStatisticsTest {

    val epsilon = 0.000000001

    val integers = 1 to 7

    val strings = List("a", "b", "c", "d", "e", "f", "g")

    val integerFrequencies = List(1, 1, 5, 1, 1, 2, 3).map(_.toDouble)

    val fractionalFrequencies: List[Double] = integerFrequencies.map(x => x / 14.toDouble)

  }

  "empty data" should "produce mode == None and weights equal to 0" in new FrequencyStatisticsTest {

    val dataList: List[Double] = List()
    val weightList: List[Double] = List()

    val dataWeightPairs = sc.parallelize(dataList.zip(weightList))

    val frequencyStats = new FrequencyStatistics[Double](dataWeightPairs)

    val testMode = frequencyStats.mode
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testMode shouldBe None
    testModeWeight shouldBe 0
    testTotalWeight shouldBe 0
  }

  "integer data with integer frequencies" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sc.parallelize(integers.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs)

    val testMode = frequencyStats.mode.get
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testMode shouldBe 3
    testModeWeight shouldBe 5
    testTotalWeight shouldBe 14
  }

  "string data with integer frequencies" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sc.parallelize(strings.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs)

    val testMode = frequencyStats.mode.get
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testMode shouldBe "c"
    testModeWeight shouldBe 5
    testTotalWeight shouldBe 14
  }

  "integer data with fractional weights" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sc.parallelize(integers.zip(fractionalFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs)

    val testMode = frequencyStats.mode.get
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testMode shouldBe 3
    Math.abs(testModeWeight - (5.toDouble / 14.toDouble)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon

  }

  "string data  with fractional weights" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sc.parallelize(strings.zip(fractionalFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs)

    val testMode = frequencyStats.mode.get
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testMode shouldBe "c"
    Math.abs(testModeWeight - (5.toDouble / 14.toDouble)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon
  }

  "items with negative weights" should "not affect mode or total weight" in new FrequencyStatisticsTest {

    val dataWeightPairs: RDD[(String, Double)] =
      sc.parallelize((strings :+ "haha").zip(fractionalFrequencies :+ ((-10.0))))

    val frequencyStats = new FrequencyStatistics[String](dataWeightPairs)

    val testMode = frequencyStats.mode.get
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testMode shouldBe "c"
    Math.abs(testModeWeight - (5.toDouble / 14.toDouble)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon
  }
}
