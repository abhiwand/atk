package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.schema.Column
import org.scalatest.WordSpec
import com.intel.intelanalytics.domain.schema.DataTypes._

class ColumnTest extends WordSpec {

  "Column" should {
    "allow alpha-numeric with underscores for column names" in {
      Column("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_", str)
    }

    "not allow - in column names" in {
      intercept[IllegalArgumentException] {
        Column("a-b", str)
      }
    }

    "not allow ' in column names" in {
      intercept[IllegalArgumentException] {
        Column("a'b", str)
      }
    }

    "not allow ? in column names" in {
      intercept[IllegalArgumentException] {
        Column("a?b", str)
      }
    }

    "not allow | in column names" in {
      intercept[IllegalArgumentException] {
        Column("a|b", str)
      }
    }

    "not allow . in column names" in {
      intercept[IllegalArgumentException] {
        Column("a.b", str)
      }
    }

    "not allow ~ in column names" in {
      intercept[IllegalArgumentException] {
        Column("a~b", str)
      }
    }

    "not allow spaces in column names" in {
      intercept[IllegalArgumentException] {
        Column("a b", str)
      }
    }

    "not allow empty column names" in {
      intercept[IllegalArgumentException] {
        Column("", str)
      }
    }

    "not allow null column names" in {
      intercept[IllegalArgumentException] {
        Column(null, str)
      }
    }

    "not allow null column types" in {
      intercept[IllegalArgumentException] {
        Column("valid_name", null)
      }
    }
  }
}
