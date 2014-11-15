package com.intel.intelanalytics.domain.command

import org.scalatest.WordSpec

class CommandPostTest extends WordSpec {

  "CommandPost" should {

    "reject invalid params" in {
      intercept[IllegalArgumentException] { CommandPost("invalid") }
    }

    "allow valid params" in {
      CommandPost("cancel")
    }
  }
}
