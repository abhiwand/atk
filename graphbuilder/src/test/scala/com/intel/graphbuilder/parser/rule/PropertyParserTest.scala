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

package com.intel.graphbuilder.parser.rule

import RuleParserDSL._
import com.intel.graphbuilder.parser.InputRow
import com.intel.graphbuilder.parser.InputSchema
import org.scalatest.{ Matchers, WordSpec }

class PropertyParserTest extends WordSpec with Matchers {

  // setup data
  val columnNames = List("id", "name", "age", "managerId", "emptyColumn", "noneColumn")
  val rowValues = List("0001", "Joe", 30, "0004", "", None)

  // setup parser dependencies
  val inputSchema = new InputSchema(columnNames, null)
  val inputRow = new InputRow(inputSchema, rowValues)

  "PropertyParser" should {

    "copy the parsed String value into the created property" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("name", column("id")))
      val property = parser.parse(inputRow)
      property.value shouldBe "0001"
    }

    "copy the parsed Int value into the created property" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("age", column("age")))
      val property = parser.parse(inputRow)
      property.value shouldBe 30
    }

    "copy the constant name into the created property" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("myname", column("id")))
      val property = parser.parse(inputRow)
      property.key shouldBe "myname"
    }

    "support parsing both the key and value from the input row" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule(column("name"), column("id")))
      val property = parser.parse(inputRow)
      property.key shouldBe "Joe"
      property.value shouldBe "0001"
    }

    "support key and values defined as constants (neither parsed from input)" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("anyName", "anyValue"))
      val property = parser.parse(inputRow)
      property.key shouldBe "anyName"
      property.value shouldBe "anyValue"
    }

    "support adding labels to vertex id's parsed from the input row" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("gbId", constant("USER.") + column("id")))
      val property = parser.parse(inputRow)
      property.key shouldBe "gbId"
      property.value shouldBe "USER.0001"
    }

    "support parsing complex compound keys and values from the input row" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule(constant("keyLabel.") + column("name") + "-" + column("id"), constant("mylabel-") + column("id")))
      val property = parser.parse(inputRow)
      property.key shouldBe "keyLabel.Joe-0001"
      property.value shouldBe "mylabel-0001"
    }

  }

  "PropertyListParser" should {

    "parse 1 property when 1 of 1 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("id", column("id")) :: Nil)
      parser.parse(inputRow).size shouldBe 1
    }

    "parse 2 properties when 2 of 2 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("id", column("id")) :: new PropertyRule("managerId", column("managerId")) :: Nil)
      parser.parse(inputRow).size shouldBe 2
    }

    "parse 1 property when 1 of 2 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("id", column("id")) :: new PropertyRule("none", column("noneColumn")) :: Nil)
      parser.parse(inputRow).size shouldBe 1
    }

    "parse 0 properties when 0 of 2 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("empty", column("emptyColumn")) :: new PropertyRule("none", column("noneColumn")) :: Nil)
      parser.parse(inputRow).size shouldBe 0
    }

    "parse 0 properties when there are no rules" in {
      val parser = new PropertyRuleParser(Nil)
      parser.parse(inputRow).size shouldBe 0
    }

  }
}
