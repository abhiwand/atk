package com.intel.intelanalytics.spray.json

import org.scalatest.FlatSpec
import JsonPropertyNameConverter.camelCaseToUnderscores

class JsonPropertyNameConverterTest extends FlatSpec {

  "JsonPropertyNameConverter" should "be able to convert CamelCase to lower-case underscore format" in {
    assert(camelCaseToUnderscores("") == "")
    assert(camelCaseToUnderscores(null) == null)
    assert(camelCaseToUnderscores("loweronly") == "loweronly")
    assert(camelCaseToUnderscores("mixedCase") == "mixed_case")
    assert(camelCaseToUnderscores("mixedCaseMultipleWords") == "mixed_case_multiple_words")
    assert(camelCaseToUnderscores("mixedCase22With55Numbers") == "mixed_case_22_with_55_numbers")
    assert(camelCaseToUnderscores("ALLUPPER") == "allupper")
    assert(camelCaseToUnderscores("PartUPPER") == "part_upper")
    assert(camelCaseToUnderscores("PARTUpper") == "part_upper")
    assert(camelCaseToUnderscores("underscores_are_unharmed") == "underscores_are_unharmed")
    assert(camelCaseToUnderscores(" needs_trim ") == "needs_trim")
    assert(camelCaseToUnderscores("FirstLetterIsCapital") == "first_letter_is_capital")
    assert(camelCaseToUnderscores(
      "aVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongName") ==
      "a_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_long_name")
  }
}
