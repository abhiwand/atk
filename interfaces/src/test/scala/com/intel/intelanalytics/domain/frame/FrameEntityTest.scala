package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec
import com.intel.intelanalytics.domain.schema.{ FrameSchema, Schema }
import org.joda.time.DateTime

class FrameEntityTest extends WordSpec {

  val frame = new FrameEntity(id = 1,
    name = Some("name"),
    status = 1,
    createdOn = new DateTime,
    modifiedOn = new DateTime)

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
        frame.copy(name = Some(""))
      }
    }

    "require a parent greater than zero" in {
      intercept[IllegalArgumentException] {
        frame.copy(parent = Some(-1))
      }
    }
  }
}
