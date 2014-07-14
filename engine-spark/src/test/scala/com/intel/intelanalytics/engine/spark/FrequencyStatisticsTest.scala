package com.intel.intelanalytics.engine.spark

import org.scalatest.{ Matchers, FunSuite }
import com.intel.intelanalytics.engine.TestingSparkContext
import org.apache.spark.SparkException

class FrequencyStatisticsITest extends TestingSparkContext with Matchers {

  trait FrequencyStatisticsTest {

    val integers = List(1, 2, 3, 3, 2, 3, 1, 1, 1, 3, 5, 7, 7, 3)
    val strings = List("a", "b", "c", "c", "b", "c", "a", "a", "a", "c", "e", "g", "g", "c")

    require(integers.length == strings.length, "Error in test data: length of integer and string lists differs.")

    val uniformWeights = integers.map(x => 1.toDouble)
    val nonUniformWrights = (1 to integers.length).map(x => x.toDouble)
  }

  "modeAndWeight" should "handle uniformly weighted integers correctly" in new FrequencyStatisticsTest {

    val dataWeightPairs = sc.parallelize(integers.zip(uniformWeights))

    val frequencyStats = new FrequencyStatistics[Int](dataWeightPairs, 0)

    val (testMode, testWeight) = frequencyStats.modeAndNetWeight

    testMode shouldBe 3
    testWeight shouldBe (5.toDouble / 14.toDouble)
  }

}
