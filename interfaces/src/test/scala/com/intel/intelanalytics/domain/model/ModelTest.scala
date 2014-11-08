package com.intel.intelanalytics.domain.model

import org.joda.time.DateTime
import org.scalatest.WordSpec

class ModelTest extends WordSpec {

  val model = new Model(1, "name", "modelType", None, 1, None, new DateTime(), new DateTime())

  "Model" should {

    "require an id greater than zero" in {
      intercept[IllegalArgumentException] { model.copy(id = -1) }
    }

    "require a name" in {
      intercept[IllegalArgumentException] { model.copy(name = null) }
    }

    "require a non-empty name" in {
      intercept[IllegalArgumentException] { model.copy(name = "") }
    }

    "require a modelType" in {
      intercept[IllegalArgumentException] { model.copy(modelType = null) }
    }

  }

}
