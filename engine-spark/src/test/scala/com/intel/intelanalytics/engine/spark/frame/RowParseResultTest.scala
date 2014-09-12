package com.intel.intelanalytics.engine.spark.frame

import org.scalatest.WordSpec

class RowParseResultTest extends WordSpec {

  "RowParseResult" should {
    "validate correct size of error frames" in {
      intercept[IllegalArgumentException] {  RowParseResult(parseSuccess = false, Array[Any](1, 2, 3)) }
    }

    "not throw errors when error frame is correct size" in {
      RowParseResult(parseSuccess = false, Array[Any]("line", "error"))
    }

    "not validate for success frames" in {
      RowParseResult(parseSuccess = true, Array[Any]("any", "number", "of", "params", "is", "okay"))
    }
  }
}
