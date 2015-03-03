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

import org.scalatest.{ BeforeAndAfter, Matchers }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.rdd.RDD

/**
 * Exercises the order statistics engine through some happy paths and a few corner cases (primarily for the case of
 * bad weights).
 */
class OrderStatisticsITest extends TestingSparkContextFlatSpec with Matchers {

  "even number of data elements" should "work" in {

    val data: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8)
    val frequencies: List[Double] = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)
    val expectedMedian: Int = 5

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sparkContext.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption.get

    testMedian shouldBe expectedMedian
  }

  "one data element" should "work" in {

    val oneThing: List[Int] = List(8)
    val oneFrequency: List[Double] = List(0.1).map(x => x.toDouble)
    val medianOfOne: Int = 8

    val numPartitions = 3
    val oneRDD: RDD[(Int, Double)] = sparkContext.parallelize(oneThing.zip(oneFrequency), numPartitions)

    val orderStatisticsForOne: OrderStatistics[Int] = new OrderStatistics[Int](oneRDD)
    val testMedian = orderStatisticsForOne.medianOption.get

    testMedian shouldBe medianOfOne
  }

  "input is two uniformly weighted items" should "result in lesser of the two values" in {

    val twoThings: List[Int] = List(8, 9)
    val frequencies: List[Double] = List(0.2, 0.2).map(x => x.toDouble)
    val expectedMedian: Int = 8

    val numPartitions = 3
    val oneRDD: RDD[(Int, Double)] = sparkContext.parallelize(twoThings.zip(frequencies), numPartitions)

    val orderStatisticsForOne: OrderStatistics[Int] = new OrderStatistics[Int](oneRDD)
    val testMedian = orderStatisticsForOne.medianOption.get

    testMedian shouldBe expectedMedian
  }

  " weights are all 0 or negative or NaN or infinite" should "return None" in {

    val data: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
    val frequencies: List[Double] = List(-3, 0, -3, 0, 0, 0, 0, 0).map(x => x.toDouble) ++
      List(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sparkContext.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption

    testMedian shouldBe None
  }

  "when weights and data are all empty" should "return None " in {

    val data: List[Int] = List()
    val frequencies: List[Double] = List()

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sparkContext.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption

    testMedian shouldBe None
  }

  "median of 1 to 100000" should "be 50,000 " in {

    val data: List[Int] = (1 to 100000).toList
    val frequencies: List[Double] = (1 to data.length).toList.map(x => 1.toDouble)

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sparkContext.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption.get

    testMedian shouldBe 50000
  }

  "median of 1 to 100001" should "be 50,001 " in {

    val data: List[Int] = (1 to 100001).toList
    val frequencies: List[Double] = (1 to data.length).toList.map(x => 1.toDouble)

    val numPartitions = 3
    val dataFrequenciesRDD: RDD[(Int, Double)] = sparkContext.parallelize(data.zip(frequencies), numPartitions)

    val dataFrequenciesOrderStatistics: OrderStatistics[Int] = new OrderStatistics[Int](dataFrequenciesRDD)

    val testMedian = dataFrequenciesOrderStatistics.medianOption.get

    testMedian shouldBe 50001
  }
}
