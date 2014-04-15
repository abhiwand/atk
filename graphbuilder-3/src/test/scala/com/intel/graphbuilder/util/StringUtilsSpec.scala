package com.intel.graphbuilder.util

import org.specs2.mutable.Specification

class StringUtilsSpec extends Specification {

  "StringUtils.nullSafeToString()" should {

    "handle null objects" in {
      StringUtils.nullSafeToString(null) mustEqual null
    }

    "handle Strings" in {
      StringUtils.nullSafeToString("anyString") mustEqual "anyString"
    }

    "handle Ints" in {
      StringUtils.nullSafeToString(1) mustEqual "1"
    }

    "handle complex objects" in {
      StringUtils.nullSafeToString(Map("my-key" -> "my-value")) mustEqual "Map(my-key -> my-value)"
    }
  }
}
