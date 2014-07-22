package com.intel.intelanalytics.engine.spark.statistics

import org.scalatest.{ BeforeAndAfter, Matchers }
import com.intel.intelanalytics.engine.TestingSparkContext
import org.apache.spark.rdd.RDD

/**
 * Exercises the order statistics engine through some happy paths and a few corner cases (primarily for the case of
 * bad weights).
 */
class OrderStatisticsITest extends TestingSparkContext with Matchers {

  "even number of data elements" should "work" in {

    val data: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8)
    val frequencies: List[Double] = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)
    val expectedMedian: Int = 5

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sc.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption.get

    testMedian shouldBe expectedMedian
  }

  "one data element" should "work" in {

    val oneThing: List[Int] = List(8)
    val oneFrequency: List[Double] = List(0.1).map(x => x.toDouble)
    val medianOfOne: Int = 8

    val numPartitions = 3
    val oneRDD: RDD[(Int, Double)] = sc.parallelize(oneThing.zip(oneFrequency), numPartitions)

    val orderStatisticsForOne: OrderStatistics[Int] = new OrderStatistics[Int](oneRDD)
    val testMedian = orderStatisticsForOne.medianOption.get

    testMedian shouldBe medianOfOne
  }

  "input is two uniformly weighted items" should "result in lesser of the two values" in {

    val twoThings: List[Int] = List(8, 9)
    val frequencies: List[Double] = List(0.2, 0.2).map(x => x.toDouble)
    val expectedMedian: Int = 8

    val numPartitions = 3
    val oneRDD: RDD[(Int, Double)] = sc.parallelize(twoThings.zip(frequencies), numPartitions)

    val orderStatisticsForOne: OrderStatistics[Int] = new OrderStatistics[Int](oneRDD)
    val testMedian = orderStatisticsForOne.medianOption.get

    testMedian shouldBe expectedMedian
  }

  " weights are all 0 or negative or NaN or infinite" should "return None" in {

    val data: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
    val frequencies: List[Double] = List(-3, 0, -3, 0, 0, 0, 0, 0).map(x => x.toDouble) ++
      List(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sc.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption

    testMedian shouldBe None
  }

  "when weights and data are all empty" should "return None " in {

    val data: List[Int] = List()
    val frequencies: List[Double] = List()

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sc.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption

    testMedian shouldBe None
  }
}
