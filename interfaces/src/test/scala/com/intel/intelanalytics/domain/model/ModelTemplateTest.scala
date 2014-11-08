package com.intel.intelanalytics.domain.model

import org.scalatest.WordSpec

class ModelTemplateTest extends WordSpec {

  "ModelTemplate" should {

    "require a name" in {
      intercept[IllegalArgumentException] { ModelTemplate(null, "OLS") }
    }

    "require a non-empty name" in {
      intercept[IllegalArgumentException] { ModelTemplate("", "OLS") }
    }

    "require a modelType" in {
      intercept[IllegalArgumentException] { ModelTemplate("name", null) }
    }

  }
}
