package com.intel.intelanalytics.engine.spark

import org.scalatest.Matchers
import com.intel.intelanalytics.engine.TestingSparkContext
import org.apache.spark.rdd.RDD

class PercentileCalculationITest extends TestingSparkContext with Matchers {
  "Calculation 40th percentile" should "return the 10th element out of 25" in {
    val numbers = List((3,Array[Any]("")),(5,Array[Any]("")),
      (6,Array[Any]("")),(7,Array[Any]("")),(23,Array[Any]("")), (8,Array[Any]("")),(21,Array[Any]("")),(9,Array[Any]("")),(11,Array[Any]("")),
      (20,Array[Any]("")),(13,Array[Any]("")),(15,Array[Any]("")),(10,Array[Any]("")),(16,Array[Any]("")),(17,Array[Any]("")),
      (18,Array[Any]("")),(1,Array[Any]("")),(19,Array[Any]("")),(4,Array[Any]("")),(22,Array[Any]("")),
      (24,Array[Any]("")),(12,Array[Any]("")),(2,Array[Any]("")),(14,Array[Any]("")),(25,Array[Any](""))
    )

    val rdd = sc.parallelize(numbers).asInstanceOf[RDD[(Any, Array[Any])]]
    val result = SparkOps.calculatePercentiles(rdd, Seq(40))
    result.length shouldBe 1
    result(0) shouldBe 10
  }

}
