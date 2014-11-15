package com.intel.intelanalytics.domain

import org.scalatest.FlatSpec
import org.joda.time.DateTime

class UserTest extends FlatSpec {

  "User" should "be able to have a none apiKey" in {
    new User(1L, None, None, new DateTime, new DateTime)
  }

  it should "not be able to have a null apiKey" in {
    intercept[IllegalArgumentException] { new User(1L, None, null, new DateTime, new DateTime) }
  }

  it should "not be able to have an empty string apiKey" in {
    intercept[IllegalArgumentException] { new User(1L, None, Some(""), new DateTime, new DateTime) }
  }

  it should "have id greater than zero" in {
    intercept[IllegalArgumentException] { new User(-1L, None, Some("api"), new DateTime, new DateTime) }
  }
}
