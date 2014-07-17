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

    val testMedian = dataFrequenciesOrderStatistics.median

    testMedian shouldBe expectedMedian
  }

  "median" should "compute the median of one thing" in {

    val oneThing: List[Int] = List(8)
    val oneFrequency: List[Double] = List(0.1).map(x => x.toDouble)
    val medianOfOne: Int = 8

    val numPartitions = 3
    val oneRDD: RDD[(Int, Double)] = sc.parallelize(oneThing.zip(oneFrequency), numPartitions)

    val orderStatisticsForOne: OrderStatistics[Int] = new OrderStatistics[Int](oneRDD)
    val testMedian = orderStatisticsForOne.median

    testMedian shouldBe medianOfOne
  }
}
