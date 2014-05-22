package com.intel.intelanalytics.engine.spark

import org.scalatest._
import org.apache.spark.SparkContext
import java.util.Date

class SparkJoinSpec extends FlatSpec with Matchers {

  "create2TupleForJoin" should "put first column in first entry" in {
    val data = Array("1", "2", 3, 4, "5")
    val result = SparkOps.create2TupleForJoin(data, 0)
    result._1 shouldBe "1"
    result._2 shouldBe Array("1", "2", 3, 4, "5")
  }

  "create2TupleForJoin" should "put third column in first entry" in {
    val data = Array("1", "2", 3, 4, "5")
    val result = SparkOps.create2TupleForJoin(data, 2)
    result._1 shouldBe 3
    result._2 shouldBe Array("1", "2", 3, 4, "5")
  }
}
