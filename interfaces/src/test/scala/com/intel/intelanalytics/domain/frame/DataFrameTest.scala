package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import com.intel.intelanalytics.domain.schema.Schema
import org.joda.time.DateTime

class DataFrameTest extends WordSpec {

  val frame = new DataFrame(1, "name", None, Schema(), 0L, 1L, new DateTime, new DateTime)

  "DataFrame" should {

    "require an id greater than zero" in {
      intercept[IllegalArgumentException] { frame.copy(id = -1) }
    }

    "require a name" in {
      intercept[IllegalArgumentException] { frame.copy(name = null) }
    }

    "require a non-empty name" in {
      intercept[IllegalArgumentException] { frame.copy(name = "") }
    }

    "require a revision greater than zero" in {
      intercept[IllegalArgumentException] { frame.copy(revision = -1) }
    }
  }
}
