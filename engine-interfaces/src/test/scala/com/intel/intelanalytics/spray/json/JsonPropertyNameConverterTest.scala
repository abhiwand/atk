//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

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
