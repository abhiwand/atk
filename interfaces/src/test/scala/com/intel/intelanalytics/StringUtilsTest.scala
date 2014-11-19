package com.intel.intelanalytics

import org.scalatest.WordSpec

class StringUtilsTest extends WordSpec {

  "StringUtils" should {

    "be true for alpha-numeric with underscores" in {
      assert(StringUtils.isAlphanumericUnderscore("a"))
      assert(StringUtils.isAlphanumericUnderscore("abc"))
      assert(StringUtils.isAlphanumericUnderscore("abc_def"))
      assert(StringUtils.isAlphanumericUnderscore("MixedCase"))
      assert(StringUtils.isAlphanumericUnderscore("Mixed_Case_With_Underscores"))
      assert(StringUtils.isAlphanumericUnderscore("_"))
      assert(StringUtils.isAlphanumericUnderscore("___"))
      assert(StringUtils.isAlphanumericUnderscore(""))
    }

    "be false for non- alpha-numeric with underscores" in {
      assert(!StringUtils.isAlphanumericUnderscore("per%cent"))
      assert(!StringUtils.isAlphanumericUnderscore("spa ce"))
      assert(!StringUtils.isAlphanumericUnderscore("doll$ar"))
      assert(!StringUtils.isAlphanumericUnderscore("pound#"))
      assert(!StringUtils.isAlphanumericUnderscore("period."))
      assert(!StringUtils.isAlphanumericUnderscore("explanation!"))
      assert(!StringUtils.isAlphanumericUnderscore("hy-phen"))
    }

  }

}
