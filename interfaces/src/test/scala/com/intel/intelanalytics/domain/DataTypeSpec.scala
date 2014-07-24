package com.intel.intelanalytics.domain

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.domain.schema.DataTypes

class DataTypeSpec extends FlatSpec with Matchers {
  "toBigDecimal" should "convert int value" in {
    val value = 100
    val bigDecimalVal = DataTypes.toBigDecimal(value)
    bigDecimalVal.intValue shouldBe value
  }

  "toBigDecimal" should "convert long value" in {
    val value: Long = 100
    val bigDecimalVal = DataTypes.toBigDecimal(value)
    bigDecimalVal.longValue shouldBe value
  }

  "toBigDecimal" should "convert float value" in {
    val value = 100.05f
    val bigDecimalVal = DataTypes.toBigDecimal(value)
    bigDecimalVal.floatValue shouldBe value
  }

  "toBigDecimal" should "convert double value" in {
    val value = 100.05
    val bigDecimalVal = DataTypes.toBigDecimal(value)
    bigDecimalVal.doubleValue shouldBe value
  }

  "toBigDecimal" should "throw Exception when non-numeric value is passed in" in {
    val value = "non-numeric"
    intercept[IllegalArgumentException] {
      DataTypes.toBigDecimal(value)
    }
  }
}
