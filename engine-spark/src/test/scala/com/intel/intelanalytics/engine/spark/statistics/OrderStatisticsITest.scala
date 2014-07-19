package com.intel.intelanalytics.engine.spark.statistics

import org.scalatest.{ BeforeAndAfter, Matchers }
import com.intel.intelanalytics.engine.TestingSparkContext
import org.apache.spark.rdd.RDD

class OrderStatisticsITest extends TestingSparkContext with Matchers {

  "median" should "compute the median of an even number of things" in {

    val data: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8)
    val frequencies: List[Double] = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)
    val expectedMedian: Int = 5

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sc.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption.get

    testMedian shouldBe expectedMedian
  }

  "median" should "compute the median of one thing" in {

    val oneThing: List[Int] = List(8)
    val oneFrequency: List[Double] = List(0.1).map(x => x.toDouble)
    val medianOfOne: Int = 8

    val numPartitions = 3
    val oneRDD: RDD[(Int, Double)] = sc.parallelize(oneThing.zip(oneFrequency), numPartitions)

    val orderStatisticsForOne: OrderStatistics[Int] = new OrderStatistics[Int](oneRDD)
    val testMedian = orderStatisticsForOne.medianOption.get

    testMedian shouldBe medianOfOne
  }

  "median" should "result in first of two uniformly weighted items" in {

    val twoThings: List[Int] = List(8,9)
    val frequencies: List[Double] = List(0.2, 0.2).map(x => x.toDouble)
    val expectedMedian: Int = 8

    val numPartitions = 3
    val oneRDD: RDD[(Int, Double)] = sc.parallelize(twoThings.zip(frequencies), numPartitions)

    val orderStatisticsForOne: OrderStatistics[Int] = new OrderStatistics[Int](oneRDD)
    val testMedian = orderStatisticsForOne.medianOption.get

    testMedian shouldBe expectedMedian
  }

  "median" should "return None when weights are all 0 or negative" in {

    val data: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8)
    val frequencies: List[Double] = List(-3, 0, -3, 0, 0, 0, 0, 0).map(x => x.toDouble)

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sc.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption

    testMedian shouldBe None
  }

  "median" should "return None when weights and data are empty" in {

    val data: List[Int] = List()
    val frequencies: List[Double] = List()

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sc.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption

    testMedian shouldBe None
  }
}
