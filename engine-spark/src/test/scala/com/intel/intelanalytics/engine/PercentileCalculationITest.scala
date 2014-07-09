package com.intel.intelanalytics.engine.spark

import org.scalatest.Matchers
import com.intel.intelanalytics.engine.TestingSparkContext
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.schema.DataTypes
import scala.collection.mutable.ListBuffer

class PercentileCalculationITest extends TestingSparkContext with Matchers {
  "Calculation percentile in small data set" should "return the correct values" in {
    val numbers = List((Array[Any](3, "")), (Array[Any](5, "")),
      (Array[Any](6, "")), (Array[Any](7, "")), (Array[Any](23, "")), (Array[Any](8, "")), (Array[Any](21, "")), (Array[Any](9, "")), (Array[Any](11, "")),
      (Array[Any](20, "")), (Array[Any](13, "")), (Array[Any](15, "")), (Array[Any](10, "")), (Array[Any](16, "")), (Array[Any](17, "")),
      (Array[Any](18, "")), (Array[Any](1, "")), (Array[Any](19, "")), (Array[Any](4, "")), (Array[Any](22, "")),
      (Array[Any](24, "")), (Array[Any](12, "")), (Array[Any](2, "")), (Array[Any](14, "")), (Array[Any](25, ""))
    )

    val rdd = sc.parallelize(numbers, 3)
    val result = SparkOps.calculatePercentiles(rdd, Seq(5, 40), 0, DataTypes.int32)
    result.length shouldBe 2
    result(0) shouldBe (5, 1.25)
    result(1) shouldBe (40, 10)
  }

  "Calculation percentile in large data set" should "return the correct values" in {

    val numbers = ListBuffer[Array[Any]]()
    numbers
    for (i <- 1 to 1000000) {
      numbers += Array[Any](i, "")
    }

    val rdd = sc.parallelize(numbers, 90)
    val result = SparkOps.calculatePercentiles(rdd, Seq(5, 40), 0, DataTypes.int32)
    result.length shouldBe 2
    result(0) shouldBe (5, 50000)
    result(1) shouldBe (40, 400000)
  }

}
