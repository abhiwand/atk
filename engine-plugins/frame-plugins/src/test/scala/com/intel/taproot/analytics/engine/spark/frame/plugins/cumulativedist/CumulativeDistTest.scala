/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.analytics.engine.spark.frame.plugins.cumulativedist

import com.intel.taproot.testutils.TestingSparkContextFlatSpec
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
