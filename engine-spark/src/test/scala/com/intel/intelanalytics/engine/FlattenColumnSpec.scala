package com.intel.intelanalytics.engine.spark

import org.scalatest.{ Matchers, FlatSpec }

class FlattenColumnSpec extends FlatSpec with Matchers {
  "flatten column" should "create multiple rows by splitting a column" in {
    val row = Array(1, "dog,cat")
    val flattened = SparkOps.flattenColumnByIndex(1, row, ",")
    flattened shouldBe Array(Array(1, "dog"), Array(1, "cat"))
  }

  "flatten column" should "not produce anything else if column is empty" in {
    val row = Array(1, "")
    val flattened = SparkOps.flattenColumnByIndex(1, row, ",")
    flattened shouldBe Array(Array(1, ""))
  }

}
