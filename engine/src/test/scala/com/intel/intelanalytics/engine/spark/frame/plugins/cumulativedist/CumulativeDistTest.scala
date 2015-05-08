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

package com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist

import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.SparkException
import org.scalatest.Matchers

class CumulativeDistTest extends TestingSparkContextFlatSpec with Matchers {

  val inputList = List(
    Array[Any](0, "a", 0),
    Array[Any](1, "b", 0),
    Array[Any](2, "c", 0),
    Array[Any](0, "a", 0),
    Array[Any](1, "b", 0),
    Array[Any](2, "c", 0))

  "cumulative sum" should "compute correct distribution" in {
    val rdd = sparkContext.parallelize(inputList)

    val resultRdd = CumulativeDistFunctions.cumulativeSum(rdd, 0)
    val result = resultRdd.take(6)

    result.apply(0) shouldBe Array[Any](0, "a", 0, 0)
    result.apply(1) shouldBe Array[Any](1, "b", 0, 1)
    result.apply(2) shouldBe Array[Any](2, "c", 0, 3)
    result.apply(3) shouldBe Array[Any](0, "a", 0, 3)
    result.apply(4) shouldBe Array[Any](1, "b", 0, 4)
    result.apply(5) shouldBe Array[Any](2, "c", 0, 6)
  }

  "cumulative sum" should "throw error for non-numeric columns" in {
    val rdd = sparkContext.parallelize(inputList)

    a[SparkException] shouldBe thrownBy(CumulativeDistFunctions.cumulativeSum(rdd, 1))
  }

  "cumulative sum" should "compute correct distribution for column of all zero" in {
    val rdd = sparkContext.parallelize(inputList)

    val resultRdd = CumulativeDistFunctions.cumulativeSum(rdd, 2)
    val result = resultRdd.take(6)

    result.apply(0) shouldBe Array[Any](0, "a", 0, 0)
    result.apply(1) shouldBe Array[Any](1, "b", 0, 0)
    result.apply(2) shouldBe Array[Any](2, "c", 0, 0)
    result.apply(3) shouldBe Array[Any](0, "a", 0, 0)
    result.apply(4) shouldBe Array[Any](1, "b", 0, 0)
    result.apply(5) shouldBe Array[Any](2, "c", 0, 0)
  }

  "cumulative count" should "compute correct distribution" in {
    val rdd = sparkContext.parallelize(inputList)

    val resultRdd = CumulativeDistFunctions.cumulativeCount(rdd, 0, "1")
    val result = resultRdd.take(6)

    result.apply(0) shouldBe Array[Any](0, "a", 0, 0)
    result.apply(1) shouldBe Array[Any](1, "b", 0, 1)
    result.apply(2) shouldBe Array[Any](2, "c", 0, 1)
    result.apply(3) shouldBe Array[Any](0, "a", 0, 1)
    result.apply(4) shouldBe Array[Any](1, "b", 0, 2)
    result.apply(5) shouldBe Array[Any](2, "c", 0, 2)
  }

  "cumulative count" should "compute correct distribution for column of all zero" in {
    val rdd = sparkContext.parallelize(inputList)

    val resultRdd = CumulativeDistFunctions.cumulativeCount(rdd, 2, "0")
    val result = resultRdd.take(6)

    result.apply(0) shouldBe Array[Any](0, "a", 0, 1)
    result.apply(1) shouldBe Array[Any](1, "b", 0, 2)
    result.apply(2) shouldBe Array[Any](2, "c", 0, 3)
    result.apply(3) shouldBe Array[Any](0, "a", 0, 4)
    result.apply(4) shouldBe Array[Any](1, "b", 0, 5)
    result.apply(5) shouldBe Array[Any](2, "c", 0, 6)
  }

  "cumulative count" should "compute correct distribution for column of strings" in {
    val rdd = sparkContext.parallelize(inputList)

    val resultRdd = CumulativeDistFunctions.cumulativeCount(rdd, 1, "b")
    val result = resultRdd.take(6)

    result.apply(0) shouldBe Array[Any](0, "a", 0, 0)
    result.apply(1) shouldBe Array[Any](1, "b", 0, 1)
    result.apply(2) shouldBe Array[Any](2, "c", 0, 1)
    result.apply(3) shouldBe Array[Any](0, "a", 0, 1)
    result.apply(4) shouldBe Array[Any](1, "b", 0, 2)
    result.apply(5) shouldBe Array[Any](2, "c", 0, 2)
  }

  "cumulative percent sum" should "compute correct distribution" in {
    val rdd = sparkContext.parallelize(inputList)

    val resultRdd = CumulativeDistFunctions.cumulativePercentSum(rdd, 0)
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result.apply(0)(3).toString) shouldEqual 0
    var diff = (java.lang.Double.parseDouble(result.apply(1)(3).toString) - 0.16666666).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result.apply(2)(3).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(3)(3).toString) shouldEqual 0.5
    diff = (java.lang.Double.parseDouble(result.apply(4)(3).toString) - 0.66666666).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result.apply(5)(3).toString) shouldEqual 1
  }

  "cumulative percent sum" should "throw error for non-numeric columns" in {
    val rdd = sparkContext.parallelize(inputList)

    a[SparkException] shouldBe thrownBy(CumulativeDistFunctions.cumulativePercentSum(rdd, 1))
  }

  "cumulative percent sum" should "compute correct distribution for column of all zero" in {
    val rdd = sparkContext.parallelize(inputList)

    val resultRdd = CumulativeDistFunctions.cumulativePercentSum(rdd, 2)
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result.apply(0)(3).toString) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(1)(3).toString) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(2)(3).toString) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(3)(3).toString) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(4)(3).toString) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(5)(3).toString) shouldEqual 1
  }

  "cumulative percent count" should "compute correct distribution" in {
    val rdd = sparkContext.parallelize(inputList)

    val resultRdd = CumulativeDistFunctions.cumulativePercentCount(rdd, 0, "1")
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result.apply(0)(3).toString) shouldEqual 0
    java.lang.Double.parseDouble(result.apply(1)(3).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(2)(3).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(3)(3).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(4)(3).toString) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(5)(3).toString) shouldEqual 1
  }

  "cumulative percent count" should "compute correct distribution for column of all zero" in {
    val rdd = sparkContext.parallelize(inputList)

    val resultRdd = CumulativeDistFunctions.cumulativePercentCount(rdd, 2, "0")
    val result = resultRdd.take(6)

    var diff = (java.lang.Double.parseDouble(result.apply(0)(3).toString) - 0.16666666).abs
    diff should be <= 0.00000001
    diff = (java.lang.Double.parseDouble(result.apply(1)(3).toString) - 0.33333333).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result.apply(2)(3).toString) shouldEqual 0.5
    diff = (java.lang.Double.parseDouble(result.apply(3)(3).toString) - 0.66666666).abs
    diff should be <= 0.00000001
    diff = (java.lang.Double.parseDouble(result.apply(4)(3).toString) - 0.83333333).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result.apply(5)(3).toString) shouldEqual 1
  }

  "cumulative percent count" should "compute correct distribution for column of strings" in {
    val rdd = sparkContext.parallelize(inputList)

    val resultRdd = CumulativeDistFunctions.cumulativePercentCount(rdd, 1, "b")
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result.apply(0)(3).toString) shouldEqual 0
    java.lang.Double.parseDouble(result.apply(1)(3).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(2)(3).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(3)(3).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(4)(3).toString) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(5)(3).toString) shouldEqual 1
  }

}
