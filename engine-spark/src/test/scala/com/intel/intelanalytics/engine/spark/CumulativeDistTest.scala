package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.engine.TestingSparkContext
import org.apache.spark.SparkException
import org.scalatest.Matchers

class CumulativeDistTest extends TestingSparkContext with Matchers {

  val inputList = List(
    Array[Any](0, "a", 0),
    Array[Any](1, "b", 0),
    Array[Any](2, "c", 0),
    Array[Any](0, "a", 0),
    Array[Any](1, "b", 0),
    Array[Any](2, "c", 0))

  "cumulative sum" should "compute correct distribution" in {
    val rdd = sc.parallelize(inputList)

    val resultRdd = SparkOps.cumulativeSum(rdd, 0)
    val result = resultRdd.take(6)

    result.apply(0) shouldBe Array[Any](0, 0)
    result.apply(1) shouldBe Array[Any](1, 1)
    result.apply(2) shouldBe Array[Any](2, 3)
    result.apply(3) shouldBe Array[Any](0, 3)
    result.apply(4) shouldBe Array[Any](1, 4)
    result.apply(5) shouldBe Array[Any](2, 6)
  }

  "cumulative sum" should "throw error for non-numeric columns" in {
    val rdd = sc.parallelize(inputList)

    a[SparkException] shouldBe thrownBy(SparkOps.cumulativeSum(rdd, 1))
  }

  "cumulative sum" should "compute correct distribution for column of all zero" in {
    val rdd = sc.parallelize(inputList)

    val resultRdd = SparkOps.cumulativeSum(rdd, 2)
    val result = resultRdd.take(6)

    result.apply(0) shouldBe Array[Any](0, 0)
    result.apply(1) shouldBe Array[Any](0, 0)
    result.apply(2) shouldBe Array[Any](0, 0)
    result.apply(3) shouldBe Array[Any](0, 0)
    result.apply(4) shouldBe Array[Any](0, 0)
    result.apply(5) shouldBe Array[Any](0, 0)
  }

  "cumulative count" should "compute correct distribution" in {
    val rdd = sc.parallelize(inputList)

    val resultRdd = SparkOps.cumulativeCount(rdd, 0, "1")
    val result = resultRdd.take(6)

    result.apply(0) shouldBe Array[Any]("0", 0)
    result.apply(1) shouldBe Array[Any]("1", 1)
    result.apply(2) shouldBe Array[Any]("2", 1)
    result.apply(3) shouldBe Array[Any]("0", 1)
    result.apply(4) shouldBe Array[Any]("1", 2)
    result.apply(5) shouldBe Array[Any]("2", 2)
  }

  "cumulative count" should "compute correct distribution for column of all zero" in {
    val rdd = sc.parallelize(inputList)

    val resultRdd = SparkOps.cumulativeCount(rdd, 2, "0")
    val result = resultRdd.take(6)

    result.apply(0) shouldBe Array[Any]("0", 1)
    result.apply(1) shouldBe Array[Any]("0", 2)
    result.apply(2) shouldBe Array[Any]("0", 3)
    result.apply(3) shouldBe Array[Any]("0", 4)
    result.apply(4) shouldBe Array[Any]("0", 5)
    result.apply(5) shouldBe Array[Any]("0", 6)
  }

  "cumulative percent sum" should "compute correct distribution" in {
    val rdd = sc.parallelize(inputList)

    val resultRdd = SparkOps.cumulativePercentSum(rdd, 0)
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result.apply(0)(1).toString()) shouldEqual 0
    var diff = (java.lang.Double.parseDouble(result.apply(1)(1).toString()) - 0.16666666).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result.apply(2)(1).toString()) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(3)(1).toString()) shouldEqual 0.5
    diff = (java.lang.Double.parseDouble(result.apply(4)(1).toString()) - 0.66666666).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result.apply(5)(1).toString()) shouldEqual 1
  }

  "cumulative percent sum" should "throw error for non-numeric columns" in {
    val rdd = sc.parallelize(inputList)

    a[SparkException] shouldBe thrownBy(SparkOps.cumulativePercentSum(rdd, 1))
  }

  "cumulative percent sum" should "compute correct distribution for column of all zero" in {
    val rdd = sc.parallelize(inputList)

    val resultRdd = SparkOps.cumulativePercentSum(rdd, 2)
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result.apply(0)(1).toString()) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(1)(1).toString()) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(2)(1).toString()) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(3)(1).toString()) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(4)(1).toString()) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(5)(1).toString()) shouldEqual 1
  }

  "cumulative percent count" should "compute correct distribution" in {
    val rdd = sc.parallelize(inputList)

    val resultRdd = SparkOps.cumulativePercentCount(rdd, 0, "1")
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result.apply(0)(1).toString()) shouldEqual 0
    java.lang.Double.parseDouble(result.apply(1)(1).toString()) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(2)(1).toString()) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(3)(1).toString()) shouldEqual 0.5
    java.lang.Double.parseDouble(result.apply(4)(1).toString()) shouldEqual 1
    java.lang.Double.parseDouble(result.apply(5)(1).toString()) shouldEqual 1
  }

  "cumulative percent count" should "compute correct distribution for column of all zero" in {
    val rdd = sc.parallelize(inputList)

    val resultRdd = SparkOps.cumulativePercentCount(rdd, 2, "0")
    val result = resultRdd.take(6)

    var diff = (java.lang.Double.parseDouble(result.apply(0)(1).toString()) - 0.16666666).abs
    diff should be <= 0.00000001
    diff = (java.lang.Double.parseDouble(result.apply(1)(1).toString()) - 0.33333333).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result.apply(2)(1).toString()) shouldEqual 0.5
    diff = (java.lang.Double.parseDouble(result.apply(3)(1).toString()) - 0.66666666).abs
    diff should be <= 0.00000001
    diff = (java.lang.Double.parseDouble(result.apply(4)(1).toString()) - 0.83333333).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result.apply(5)(1).toString()) shouldEqual 1
  }

}
