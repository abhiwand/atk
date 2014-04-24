//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.graphbuilder.parser

import com.intel.graphbuilder.parser.rule.{CompoundValue, ParsedValue, ConstantValue, Value}
import org.apache.commons.lang3.StringUtils
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.mock.Mockito

class ValueSpec extends Specification with ScalaCheck with Mockito {

  "Value" should {

    "support constant hard-coded values" in {
      val hardcoded = "hardcoded"
      val value = new ConstantValue(hardcoded)
      value.value mustEqual hardcoded
    }
  }

  "ConstantValue" should {
    "never be considered parsed" in {
      new ConstantValue("any").isParsed mustEqual false
    }

    "never be considerd parsed for any value" ! check {
      (a: String) =>
        StringUtils.isNotEmpty(a) ==>
          !new ConstantValue(a).isParsed
    }
  }

  "ParsedValue" should {
    "always be considered parsed" in {
      new ParsedValue("any").isParsed mustEqual true
    }

    "throw an exception for empty column names" in {
      new ParsedValue("") must throwA[IllegalArgumentException]
    }

    "throw an exception for null column names" in {
      new ParsedValue(null) must throwA[IllegalArgumentException]
    }

    "throw an exception if value() is called without a row" in {
      new ParsedValue("any").value must throwA[RuntimeException]
    }

    "always be considered parsed for any value" ! check {
      (a: String) =>
        StringUtils.isNotEmpty(a) ==>
          new ParsedValue(a).isParsed
    }
  }

  "CompoundValue" should {
    "never be considered parsed when fully composed of constant values" in {
      new CompoundValue(new ConstantValue("any"), new ConstantValue("any")).isParsed mustEqual false
    }

    "always be considered parsed when first value is a parsed value" in {
      new CompoundValue(new ParsedValue("any"), new ConstantValue("any")).isParsed mustEqual true
    }

    "always be considered parsed when second value is a parsed value" in {
      new CompoundValue(new ConstantValue("any"), new ParsedValue("any")).isParsed mustEqual true
    }

    "always be considered parsed when fully composed of parsed values" in {
      new CompoundValue(new ParsedValue("any"), new ParsedValue("any")).isParsed mustEqual true
    }

    "never be considered parsed when fully composed of constant values" ! check {
      (a: String, b: String) =>
        (StringUtils.isNotEmpty(a) && StringUtils.isNotEmpty(b)) ==>
          !new CompoundValue(new ConstantValue(a), new ConstantValue(b)).isParsed
    }

    "delegate 'in' to underlying values" in {
      // setup mocks
      val row = mock[InputRow]
      val valueInTrue = mock[Value]
      val valueInFalse = mock[Value]
      valueInTrue.in(row) returns true
      valueInFalse.in(row) returns false

      // invoke method under test
      new CompoundValue(valueInTrue, valueInTrue).in(row) mustEqual true
      new CompoundValue(valueInTrue, valueInFalse).in(row) mustEqual false
      new CompoundValue(valueInFalse, valueInTrue).in(row) mustEqual false
      new CompoundValue(valueInFalse, valueInFalse).in(row) mustEqual false
    }

    "concatenate underlying values" in {
      // setup mocks
      val row = mock[InputRow]
      val value1 = mock[Value]
      val value2 = mock[Value]
      value1.value returns "111"
      value2.value returns "222"

      // invoke method under test
      new CompoundValue(value1, value2).value mustEqual "111222"
    }

    "concatenate underlying values not dying on null constants" in {
      // invoke method under test
      new CompoundValue(new ConstantValue(null), new ConstantValue(null)).value mustEqual "null"
    }
  }

}
