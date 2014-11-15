package com.intel.intelanalytics.domain.frame

import org.scalatest.WordSpec

class DataFrameTemplateTest extends WordSpec {

  "DataFrameTemplate" should {

    "require a name" in {
      intercept[IllegalArgumentException] { DataFrameTemplate(null, None) }
    }

    "require a non-empty name" in {
      intercept[IllegalArgumentException] { DataFrameTemplate("", None) }
    }

    "require a non-null description" in {
      intercept[IllegalArgumentException] { DataFrameTemplate("name", null) }
    }
  }
}
