package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import com.intel.intelanalytics.domain.schema.Schema
import org.joda.time.DateTime

class DataFrameTest extends WordSpec {

  val frame = new DataFrame(id = 1,
    name = "name",
    status = 1,
    createdOn = new DateTime)

  "DataFrame" should {

    "require an id greater than zero" in {
      intercept[IllegalArgumentException] {
        frame.copy(id = -1)
      }
    }

    "require a name" in {
      intercept[IllegalArgumentException] {
        frame.copy(name = null)
      }
    }

    "require a non-empty name" in {
      intercept[IllegalArgumentException] {
        frame.copy(name = "")
      }
    }

    "require a parent greater than zero" in {
      intercept[IllegalArgumentException] {
        frame.copy(parent = Some(-1))
      }
    }
  }
}
