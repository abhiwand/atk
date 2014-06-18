package com.intel.intelanalytics.domain

import org.scalatest.FlatSpec
import org.joda.time.DateTime

class UserSpec extends FlatSpec {

  "User" should "be able to have a none apiKey" in {
    new User(1L, None, None, new DateTime, new DateTime)
  }
}
