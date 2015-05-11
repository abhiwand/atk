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

package com.intel.graphbuilder.parser

import org.scalatest.{ Matchers, WordSpec }
import com.intel.graphbuilder.parser.rule.{ Value, ParsedValue, CompoundValue, ConstantValue }
import org.mockito.Mockito._
import com.intel.graphbuilder.parser.rule.ParsedValue
import com.intel.graphbuilder.parser.rule.CompoundValue
import com.intel.graphbuilder.parser.rule.ConstantValue
import org.scalatest.mock.MockitoSugar

class ValueTest extends WordSpec with Matchers with MockitoSugar {

  "Value" should {

    "support constant hard-coded values" in {
      val hardcoded = "hardcoded"
      val value = new ConstantValue(hardcoded)
      value.value shouldBe hardcoded
    }
  }

  "ConstantValue" should {
    "never be considered parsed (String)" in {
      new ConstantValue("any").isParsed shouldBe false
    }

    "never be considered parsed (Int)" in {
      new ConstantValue(0).isParsed shouldBe false
    }

    "never be considered parsed (empty list)" in {
      new ConstantValue(Nil).isParsed shouldBe false
    }
  }

  "ParsedValue" should {
    "always be considered parsed" in {
      new ParsedValue("any").isParsed shouldBe true
    }

    "throw an exception for empty column names" in {
      an[IllegalArgumentException] should be thrownBy new ParsedValue("")
    }

    "throw an exception for null column names" in {
      an[IllegalArgumentException] should be thrownBy new ParsedValue(null)
    }

    "throw an exception if value() is called without a row" in {
      an[RuntimeException] should be thrownBy new ParsedValue("any").value
    }

  }

  "CompoundValue" should {
    "never be considered parsed when fully composed of constant values" in {
      new CompoundValue(new ConstantValue("any"), new ConstantValue("any")).isParsed shouldBe false
    }

    "always be considered parsed when first value is a parsed value" in {
      new CompoundValue(new ParsedValue("any"), new ConstantValue("any")).isParsed shouldBe true
    }

    "always be considered parsed when second value is a parsed value" in {
      new CompoundValue(new ConstantValue("any"), new ParsedValue("any")).isParsed shouldBe true
    }

    "always be considered parsed when fully composed of parsed values" in {
      new CompoundValue(new ParsedValue("any"), new ParsedValue("any")).isParsed shouldBe true
    }

    "delegate 'in' to underlying values" in {
      // setup mocks
      val row = mock[InputRow]
      val valueInTrue = mock[Value]
      val valueInFalse = mock[Value]
      when(valueInTrue.in(row)).thenReturn(true)
      when(valueInFalse.in(row)).thenReturn(false)

      // invoke method under test
      new CompoundValue(valueInTrue, valueInTrue).in(row) shouldBe true
      new CompoundValue(valueInTrue, valueInFalse).in(row) shouldBe false
      new CompoundValue(valueInFalse, valueInTrue).in(row) shouldBe false
      new CompoundValue(valueInFalse, valueInFalse).in(row) shouldBe false
    }

    "concatenate underlying values" in {
      // setup mocks
      val value1 = mock[Value]
      val value2 = mock[Value]
      when(value1.value).thenReturn("111", None)
      when(value2.value).thenReturn("222", None)

      // invoke method under test
      new CompoundValue(value1, value2).value shouldBe "111222"
    }

    "concatenate underlying values not dying on null constants" in {
      // invoke method under test
      new CompoundValue(new ConstantValue(null), new ConstantValue(null)).value shouldBe "null"
    }
  }

}
